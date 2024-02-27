// Copyright 2017-2021 Bitalostored author and other contributors.
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

package order

import (
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalostored/raft/logger"
)

var (
	plog = logger.GetLogger("order")
)

type NodeState struct {
	nlastHBTime int64  // 上次心跳时间
	nCommit     uint64 // 主的 commit 状态
	nLastIndex  uint64 //从的
	nTerm       uint64
	nIsLeader   int32
	nIsWitness  int32
	nRecover    int
	nOk         int32
}

type NodeStateInfo struct {
	LastHBTime int64  `json:"last_hb_time"`
	Commit     uint64 `json:"commit"`
	LastIndex  uint64 `json:"last_index"`
	IsLeader   int32  `json:"is_leader"`
	IsWitness  int32  `json:"is_witness"`
	Recover    int    `json:"recover"`
	Diff       int64  `json:"diff"`
}

func (p *NodeState) Ok(nNow, hbPeriod int64) int32 {
	bhInterval := nNow - atomic.LoadInt64(&p.nlastHBTime)
	if bhInterval > hbPeriod {
		plog.Infof(" heartbeat error. actual interval: %d, expect interval: %d", bhInterval, hbPeriod)
		return 0
	} else if atomic.LoadInt32(&p.nIsWitness) > 0 {
		return 1
	}

	if atomic.LoadInt32(&p.nIsLeader) > 0 {
		return 1
	}

	if p.nRecover == 1 {
		return 0
	}

	return 1
}

type NodeStates struct {
	mapRuningStat map[uint64]*NodeState
	//nLastUpdateTime int64
	nUpdateIntervel int64
	nHBInterval     int64
	nLen            int64
}

func NewNodeStates() *NodeStates {
	//now := time.Now().UnixNano()
	return &NodeStates{
		mapRuningStat: make(map[uint64]*NodeState),
		//nLastUpdateTime: now,
		nUpdateIntervel: int64(1 * time.Second),
		nHBInterval:     int64(1 * time.Second),
		nLen:            100,
	}
}

var G_NodeSates *NodeStates = NewNodeStates()

func (p *NodeStates) Add(nClusterId uint64) {
	p.mapRuningStat[nClusterId] = &NodeState{}
}

func (p *NodeStates) SetPara(nUpdateInterval, nHBInterval, nLen int64) {
	p.nUpdateIntervel = nUpdateInterval
	p.nHBInterval = nHBInterval
	p.nLen = nLen
}

func (p *NodeStates) SetStates(nClusterId, nCommit, nLastIndex, nTerm uint64, nTime int64, bIsLeader, bIsWitness bool) {
	pNS := p.getNodeState(nClusterId)
	if nil == pNS {
		return
	}

	atomic.SwapInt64(&pNS.nlastHBTime, nTime)
	atomic.SwapUint64(&pNS.nCommit, nCommit)
	atomic.SwapUint64(&pNS.nLastIndex, nLastIndex)
	atomic.SwapUint64(&pNS.nTerm, nTerm)

	if bIsLeader {
		atomic.SwapInt32(&pNS.nIsLeader, 1)
	} else {
		atomic.SwapInt32(&pNS.nIsLeader, 0)
	}

	if bIsWitness {
		atomic.SwapInt32(&pNS.nIsWitness, 1)
	} else {
		atomic.SwapInt32(&pNS.nIsWitness, 0)
	}
}

func (p *NodeStates) SetRecover(nClusterId uint64, in int) {
	pNS := p.getNodeState(nClusterId)
	if nil == pNS {
		return
	}
	pNS.nRecover = in
}

func (p *NodeStates) OK(nClusterId uint64) bool {
	pNS := p.getNodeState(nClusterId)
	if nil == pNS {
		plog.Infof("pns is nil")
		return false
	}

	return pNS.Ok(time.Now().UnixNano(), p.nHBInterval) > 0
}

func (p *NodeStates) StatInfo(nClusterId uint64) NodeStateInfo {
	pNS := p.getNodeState(nClusterId)
	if nil == pNS {
		return NodeStateInfo{}
	}
	return NodeStateInfo{
		LastHBTime: atomic.LoadInt64(&pNS.nlastHBTime),
		Commit:     atomic.LoadUint64(&pNS.nCommit),
		LastIndex:  atomic.LoadUint64(&pNS.nLastIndex),
		IsLeader:   atomic.LoadInt32(&pNS.nIsLeader),
		IsWitness:  atomic.LoadInt32(&pNS.nIsWitness),
		Recover:    pNS.nRecover,
		Diff:       int64(atomic.LoadUint64(&pNS.nLastIndex)) - int64(atomic.LoadUint64(&pNS.nCommit)),
	}
}

func (p *NodeStates) GetUpdateInterval() int64 {
	return p.nUpdateIntervel
}

func (p *NodeStates) GetTerm(nClusterId uint64) uint64 {
	v, ok := p.mapRuningStat[nClusterId]
	if !ok {
		return 0
	}
	return atomic.LoadUint64(&v.nTerm)
}

func (p *NodeStates) GetCommit(nClusterId uint64) uint64 {
	v, ok := p.mapRuningStat[nClusterId]
	if !ok {
		return 0
	}
	return atomic.LoadUint64(&v.nCommit)
}

func (p *NodeStates) getNodeState(nClusterId uint64) *NodeState {
	v, ok := p.mapRuningStat[nClusterId]
	if !ok {
		return nil
	}
	return v
}
