// Copyright 2017-2020 Lei Ni (nilei81@gmail.com), Bitalostored author and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//
// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//
// Peer.go is the interface used by the upper layer to access functionalities
// provided by the raft protocol. It translates all incoming requests to raftpb
// messages and pass them to the raft protocol implementation to be handled.
// Such a state machine style design together with the iterative style interface
// here is derived from etcd.
// Compared to etcd raft, we strictly model all inputs to the raft protocol as
// messages including those used to advance the raft state.
//

package raft

import (
	"sort"

	"github.com/zuoyebang/bitalostored/raft/config"
	"github.com/zuoyebang/bitalostored/raft/internal/server"
	pb "github.com/zuoyebang/bitalostored/raft/raftpb"
)

// PeerAddress is the basic info for a peer in the Raft cluster.
type PeerAddress struct {
	Address string
	NodeID  uint64
}

// Peer is the interface struct for interacting with the underlying Raft
// protocol implementation.
type Peer struct {
	raft      *raft
	prevState pb.State
}

// Launch starts or restarts a Raft node.
func Launch(config config.Config,
	logdb ILogDB, events server.IRaftEventListener,
	addresses []PeerAddress, initial bool, newNode bool) Peer {
	checkLaunchRequest(config, addresses, initial, newNode)
	plog.Infof("%s created, initial: %t, new: %t",
		dn(config.ClusterID, config.NodeID), initial, newNode)
	p := Peer{raft: newRaft(config, logdb, addresses...)}
	p.raft.events = events
	p.prevState = p.raft.raftState()

	if initial && newNode {
		//p.raft.becomeFollower(1, NoLeader)
		bootstrap(p.raft, addresses)
	}
	return p
}

//func (p *Peer) GetLogCommitedIndex() uint64 {
//	return p.raft.getLogCommitedIndex()
//}
//
//func (p *Peer) SetLogCommitedIndex(idx uint64) {
//	p.raft.setLogCommitedIndex(idx)
//}

func (p *Peer) HasSnapshot() bool {
	return p.raft.hasSnapshot
}

// Tick moves the logical clock forward by one tick.
func (p *Peer) Tick() error {
	return p.raft.Handle(&pb.Message{
		Type:   pb.LocalTick,
		Reject: false,
	})
}

// QuiescedTick moves the logical clock forward by one tick in quiesced mode.
func (p *Peer) QuiescedTick() error {
	return p.raft.Handle(&pb.Message{
		Type:   pb.LocalTick,
		Reject: true,
	})
}

// RequestLeaderTransfer makes a request to transfer the leadership to the
// specified target node.
func (p *Peer) RequestLeaderTransfer(target uint64) error {
	return p.raft.Handle(&pb.Message{
		Type: pb.LeaderTransfer,
		To:   p.raft.nodeID,
		Hint: target,
	})
}

// ProposeEntries proposes specified entries in a batched mode using a single
// MTPropose message.
func (p *Peer) ProposeEntries(ents []pb.Entry) error {
	return p.raft.Handle(&pb.Message{
		Type:    pb.Propose,
		From:    p.raft.nodeID,
		Entries: ents,
	})
}

// ProposeConfigChange proposes a raft membership change.
func (p *Peer) ProposeConfigChange(cc pb.ConfigChange, key uint64) error {
	data := pb.MustMarshal(&cc)
	return p.raft.Handle(&pb.Message{
		Type:    pb.Propose,
		Entries: []pb.Entry{{Type: pb.ConfigChangeEntry, Cmd: data, Key: key}},
	})
}

// ApplyConfigChange applies a raft membership change to the local raft node.
func (p *Peer) ApplyConfigChange(cc pb.ConfigChange) error {
	if cc.NodeID == NoLeader {
		p.raft.clearPendingConfigChange()
		return nil
	}
	return p.raft.Handle(&pb.Message{
		Type:     pb.ConfigChangeEvent,
		Reject:   false,
		Hint:     cc.NodeID,
		HintHigh: uint64(cc.Type),
	})
}

// RejectConfigChange rejects the currently pending raft membership change.
func (p *Peer) RejectConfigChange() error {
	return p.raft.Handle(&pb.Message{
		Type:   pb.ConfigChangeEvent,
		Reject: true,
	})
}

// RestoreRemotes applies the remotes info obtained from the specified snapshot.
func (p *Peer) RestoreRemotes(ss pb.Snapshot) error {
	return p.raft.Handle(&pb.Message{
		Type:     pb.SnapshotReceived,
		Snapshot: ss,
	})
}

// ReportUnreachableNode marks the specified node as not reachable.
func (p *Peer) ReportUnreachableNode(nodeID uint64) error {
	return p.raft.Handle(&pb.Message{
		Type: pb.Unreachable,
		From: nodeID,
	})
}

// ReportSnapshotStatus reports the status of the snapshot to the local raft
// node.
func (p *Peer) ReportSnapshotStatus(nodeID uint64, reject bool) error {
	return p.raft.Handle(&pb.Message{
		Type:   pb.SnapshotStatus,
		From:   nodeID,
		Reject: reject,
	})
}

// Handle processes the given message.
func (p *Peer) Handle(m *pb.Message) error {
	if IsLocalMessageType(m.Type) {
		panic("local message sent to Step")
	}
	_, rok := p.raft.remotes[m.From]
	_, ook := p.raft.nonVotings[m.From]
	_, wok := p.raft.witnesses[m.From]
	if rok || ook || wok || !isResponseMessageType(m.Type) {
		return p.raft.Handle(m)
	}
	return nil
}

// GetUpdate returns the current state of the Peer.
func (p *Peer) GetUpdate(moreToApply bool,
	lastApplied uint64) (pb.Update, error) {
	ud, err := p.getUpdate(moreToApply, lastApplied)
	if err != nil {
		return pb.Update{}, err
	}
	validateUpdate(ud)
	ud = setFastApply(ud)
	ud.UpdateCommit = getUpdateCommit(ud)
	return ud, nil
}

func (p *Peer) GetUpdateForFlush(lower, upper uint64) ([]pb.Update, error) {
	if lower >= upper {
		return []pb.Update{}, nil
	}

	uds := make([]pb.Update, 0, 10)
	var ud pb.Update
	var err error
	toApply := lower
	plog.Infof("get entry from logdb(replay). lower:%d upper:%d", lower, upper)
	var firstFind, lastFind uint64
	for {
		ud, err = p.getUpdateForFlush(true, toApply, upper)
		if err != nil {
			plog.Infof("get entry from logdb(replay). lower:%d upper:%d err:%+v", lower, upper, err)
			break
		}

		commitLen := len(ud.CommittedEntries)
		if commitLen > 0 {
			firstFind = ud.CommittedEntries[0].Index
			lastFind = ud.CommittedEntries[commitLen-1].Index
		} else {
			firstFind = 0
			lastFind = 0
		}
		plog.Infof("getUpdateForFlush(replay) toApply:%d lastApplied:%d entry.first:%d, entry.last:%d entry.len:%d", toApply, upper, firstFind, lastFind, commitLen)

		validateUpdate(ud)
		ud = setFastApply(ud)
		ud.UpdateCommit = getUpdateCommit(ud)
		uds = append(uds, ud)
		if lastFind >= upper-1 || commitLen == 0 {
			break
		}
		toApply = lastFind + 1
	}
	return uds, nil
}

func setFastApply(ud pb.Update) pb.Update {
	ud.FastApply = true
	if !pb.IsEmptySnapshot(ud.Snapshot) {
		ud.FastApply = false
	}
	if ud.FastApply {
		if len(ud.CommittedEntries) > 0 && len(ud.EntriesToSave) > 0 {
			lastApplyIndex := ud.CommittedEntries[len(ud.CommittedEntries)-1].Index
			lastSaveIndex := ud.EntriesToSave[len(ud.EntriesToSave)-1].Index
			firstSaveIndex := ud.EntriesToSave[0].Index
			if lastApplyIndex >= firstSaveIndex && lastApplyIndex <= lastSaveIndex {
				ud.FastApply = false
			}
		}
	}
	return ud
}

func validateUpdate(ud pb.Update) {
	if ud.Commit > 0 && len(ud.CommittedEntries) > 0 {
		lastIndex := ud.CommittedEntries[len(ud.CommittedEntries)-1].Index
		if lastIndex > ud.Commit {
			plog.Panicf("trying to apply not committed entry: %d, %d",
				ud.Commit, lastIndex)
		}
	}
	if len(ud.CommittedEntries) > 0 && len(ud.EntriesToSave) > 0 {
		lastApply := ud.CommittedEntries[len(ud.CommittedEntries)-1].Index
		lastSave := ud.EntriesToSave[len(ud.EntriesToSave)-1].Index
		if lastApply > lastSave {
			plog.Panicf("trying to apply not saved entry: %d, %d",
				lastApply, lastSave)
		}
	}
}

func (p *Peer) SetInitLogEntryOnDiskIndex(index uint64) bool {
	plog.Infof("init ondisk index : %d", index)
	p.raft.log.setLogEntryOnDiskIndex(index)
	return true
}

func (p *Peer) GetInitLogEntryCommitedIndex() uint64 {
	commited := p.raft.log.getLogEntryCommitedIndex()
	plog.Infof("get init log enrty commited index : %d", commited)
	return commited
}

// RateLimited returns a boolean flag indicating whether the Raft node is rate
// limited.
func (p *Peer) RateLimited() bool {
	return p.raft.rl.RateLimited()
}

// HasUpdate returns a boolean value indicating whether there is any Update
// ready to be processed.
func (p *Peer) HasUpdate(moreToApply bool) bool {
	r := p.raft
	if len(r.log.entriesToSave()) > 0 {
		return true
	}
	if len(r.msgs) > 0 {
		return true
	}
	if moreToApply && r.log.hasEntriesToApply() {
		return true
	}
	if pst := r.raftState(); !pb.IsEmptyState(pst) &&
		!pb.IsStateEqual(pst, p.prevState) {
		return true
	}
	if r.log.inmem.snapshot != nil &&
		!pb.IsEmptySnapshot(*r.log.inmem.snapshot) {
		return true
	}
	if len(r.readyToRead) != 0 {
		return true
	}
	if len(r.droppedEntries) > 0 {
		return true
	}
	if len(r.droppedReadIndexes) > 0 {
		return true
	}
	return false
}

// Commit commits the Update state to mark it as processed.
func (p *Peer) Commit(ud pb.Update) {
	p.raft.msgs = nil
	p.raft.droppedEntries = nil
	p.raft.droppedReadIndexes = nil
	if !pb.IsEmptyState(ud.State) {
		p.prevState = ud.State
	}
	if ud.UpdateCommit.ReadyToRead > 0 {
		p.raft.clearReadyToRead()
	}
	p.entryLog().commitUpdate(ud.UpdateCommit)
}

// ReadIndex starts a ReadIndex operation. The ReadIndex protocol is defined in
// the section 6.4 of the Raft thesis.
func (p *Peer) ReadIndex(ctx pb.SystemCtx) error {
	return p.raft.Handle(&pb.Message{
		Type:     pb.ReadIndex,
		Hint:     ctx.Low,
		HintHigh: ctx.High,
	})
}

// NotifyRaftLastApplied passes on the lastApplied index confirmed by the RSM to
// the raft state machine.
func (p *Peer) NotifyRaftLastApplied(lastApplied uint64) {
	p.raft.setApplied(lastApplied)
}

// HasEntryToApply returns a boolean flag indicating whether there are more
// entries ready to be applied.
func (p *Peer) HasEntryToApply() bool {
	return p.entryLog().hasEntriesToApply()
}

func (p *Peer) ForbidRemoveLog(compactTo uint64) bool {
	if !p.raft.isLeader() {
		return false
	}
	for n, rp := range p.raft.remotes {
		if p.raft.nodeID == n {
			continue
		}
		if rp.match < compactTo {
			plog.Infof("remote log not enough. remote:%d compactTo:%d remoteIndex:%d", n, compactTo, rp.match)
			return true
		}
	}
	for n, rp := range p.raft.nonVotings {
		if rp.match < compactTo {
			plog.Infof("remote log not enough. remote:%d compactTo:%d remoteIndex:%d", n, compactTo, rp.match)
			return true
		}
	}
	return false
}

func (p *Peer) entryLog() *entryLog {
	return p.raft.log
}

func (p *Peer) SetSelfRecover(recover bool) {
	p.raft.setSelfRecover(recover)
}

func (p *Peer) getUpdate(moreToApply bool,
	lastApplied uint64) (pb.Update, error) {
	ud := pb.Update{
		ClusterID:     p.raft.clusterID,
		NodeID:        p.raft.nodeID,
		EntriesToSave: p.entryLog().entriesToSave(),
		Messages:      p.raft.msgs,
		LastApplied:   lastApplied,
		FastApply:     true,
	}
	for idx := range ud.Messages {
		ud.Messages[idx].ClusterId = p.raft.clusterID
	}
	if moreToApply {
		toApply, err := p.entryLog().entriesToApply()
		if err != nil {
			return pb.Update{}, err
		}
		ud.CommittedEntries = toApply
	}
	if len(ud.CommittedEntries) > 0 {
		lastIndex := ud.CommittedEntries[len(ud.CommittedEntries)-1].Index
		ud.MoreCommittedEntries = p.entryLog().hasMoreEntriesToApply(lastIndex)
	}
	if pst := p.raft.raftState(); !pb.IsStateEqual(pst, p.prevState) {
		ud.State = pst
	}
	if p.entryLog().inmem.snapshot != nil {
		ud.Snapshot = *p.entryLog().inmem.snapshot
	}
	if len(p.raft.readyToRead) > 0 {
		ud.ReadyToReads = p.raft.readyToRead
	}
	if len(p.raft.droppedEntries) > 0 {
		ud.DroppedEntries = p.raft.droppedEntries
	}
	if len(p.raft.droppedReadIndexes) > 0 {
		ud.DroppedReadIndexes = p.raft.droppedReadIndexes
	}
	return ud, nil
}

// 包含左边flushIndex 不包含右边lastApplied
func (p *Peer) getUpdateForFlush(moreToApply bool,
	flushIndex, lastApplied uint64) (pb.Update, error) {
	ud := pb.Update{
		ClusterID:   p.raft.clusterID,
		NodeID:      p.raft.nodeID,
		Messages:    p.raft.msgs,
		LastApplied: lastApplied,
		FastApply:   true,
	}
	for idx := range ud.Messages {
		ud.Messages[idx].ClusterId = p.raft.clusterID
	}
	if moreToApply {
		toApply, err := p.entryLog().getEntriesForFlush(flushIndex, lastApplied)
		if err != nil {
			return pb.Update{}, err
		}
		ud.CommittedEntries = toApply
	}
	if len(ud.CommittedEntries) > 0 {
		lastIndex := ud.CommittedEntries[len(ud.CommittedEntries)-1].Index
		ud.MoreCommittedEntries = p.entryLog().hasMoreEntriesToApply(lastIndex)
	}
	if p.entryLog().inmem.snapshot != nil {
		ud.Snapshot = *p.entryLog().inmem.snapshot
	}
	if len(p.raft.readyToRead) > 0 {
		ud.ReadyToReads = p.raft.readyToRead
	}
	if len(p.raft.droppedEntries) > 0 {
		ud.DroppedEntries = p.raft.droppedEntries
	}
	if len(p.raft.droppedReadIndexes) > 0 {
		ud.DroppedReadIndexes = p.raft.droppedReadIndexes
	}
	return ud, nil
}

func checkLaunchRequest(config config.Config,
	addresses []PeerAddress, initial bool, newNode bool) {
	if config.NodeID == 0 {
		panic("config.NodeID must not be zero")
	}
	if initial && newNode && len(addresses) == 0 {
		panic("addresses must be specified")
	}
	uniqueAddressList := make(map[string]struct{})
	for _, addr := range addresses {
		uniqueAddressList[addr.Address] = struct{}{}
	}
	if len(uniqueAddressList) != len(addresses) {
		plog.Panicf("duplicated address found %v", addresses)
	}
	if initial && config.IsWitness {
		plog.Panicf("witness can not be used as initial member")
	}
	if initial && config.IsNonVoting {
		plog.Panicf("non-voting can not be used as initial member")
	}
}

func bootstrap(r *raft, addresses []PeerAddress) {
	sort.Slice(addresses, func(i, j int) bool {
		return addresses[i].NodeID < addresses[j].NodeID
	})
	ents := make([]pb.Entry, len(addresses))
	for i, peer := range addresses {
		plog.Infof("%s added bootstrap ConfigChangeAddNode, %d, %s",
			r.describe(), peer.NodeID, peer.Address)
		cc := pb.ConfigChange{
			Type:       pb.AddNode,
			NodeID:     peer.NodeID,
			Initialize: true,
			Address:    peer.Address,
		}
		ents[i] = pb.Entry{
			Type:  pb.ConfigChangeEntry,
			Term:  1,
			Index: uint64(i + 1),
			Cmd:   pb.MustMarshal(&cc),
		}
	}
	r.log.append(ents)
	r.log.committed = uint64(len(ents))
	for _, peer := range addresses {
		r.addNode(peer.NodeID)
	}
}

func getUpdateCommit(ud pb.Update) pb.UpdateCommit {
	uc := pb.UpdateCommit{
		ReadyToRead: uint64(len(ud.ReadyToReads)),
		LastApplied: ud.LastApplied,
	}
	if len(ud.CommittedEntries) > 0 {
		uc.Processed = ud.CommittedEntries[len(ud.CommittedEntries)-1].Index
	}
	if len(ud.EntriesToSave) > 0 {
		lastEntry := ud.EntriesToSave[len(ud.EntriesToSave)-1]
		uc.StableLogTo, uc.StableLogTerm = lastEntry.Index, lastEntry.Term
	}
	if !pb.IsEmptySnapshot(ud.Snapshot) {
		uc.StableSnapshotTo = ud.Snapshot.Index
		uc.Processed = max(uc.Processed, uc.StableSnapshotTo)
	}
	return uc
}
