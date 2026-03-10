// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
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

package raft

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalostored/stored/engine/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/cmd"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/server"

	"github.com/zuoyebang/bitalosraft"
	dconfig "github.com/zuoyebang/bitalosraft/config"
	"github.com/zuoyebang/bitalosraft/logger"
	"github.com/zuoyebang/bitalosraft/order"
)

const (
	DefaultElectionRTT = 35

	DefaultMaxInMemLogSize     = 4 << 30
	DefaultMaxSendQueueSize    = 1 << 30
	DefaultMaxReceiveQueueSize = 1 << 30
)

var GlobalMultiRaft *MultiRaft = &MultiRaft{}
var defaultClusterConfig *config.FixedRaftClusterConfig

type MultiRaft struct {
	Nhc dconfig.NodeHostConfig
	cfg *config.Config
	s   *server.Server

	metaDir     string
	nodehost    *bitalosraft.NodeHost
	nodehostDir string
	walDir      string

	mu struct {
		sync.RWMutex
		clusterRaftGroup map[uint64]*RaftNode
		onlineClusterId  uint64 // num of online raft group
	}

	statRunning bool
	mapping     *SlotMapping
	raftMeta    *RaftMeta

	logCompact   atomic.Uint32
	deraftFlag   atomic.Uint32
	deraftLock   sync.RWMutex
	deraftInfo   *RaftInfo
	retryTimes   int
	closed       atomic.Bool
	queue        *Queue
	refreshOnce  sync.Once
	refreshBuf   *bytes.Buffer
	refreshNodes []string
}

func InitRaftInstance(c *config.Config, s *server.Server) error {
	GlobalMultiRaft.cfg = c
	s.DoRaftStop = GlobalMultiRaft.close
	s.IsMaster = GlobalMultiRaft.isMaster
	s.GetRaftInfo = GlobalMultiRaft.GetRaftInfo
	s.SetRaftNodeState = SetRaftNodeState
	GlobalMultiRaft.registerRaftCommand(s)

	if !c.Plugin.OpenRaft || c.CheckIsDegradeSingleNode() {
		GlobalMultiRaft.deraftInit()
		return nil
	}
	return startNodehost(c, s)
}

func startNodehost(c *config.Config, s *server.Server) error {
	setNodeInitDir(config.GetRaftConfigPath())

	GlobalMultiRaft.queue = NewQueue(c.RaftQueue.Workers, c.RaftQueue.Length, s)
	GlobalMultiRaft.s = s
	GlobalMultiRaft.retryTimes = c.RaftCluster.RetryTimes
	GlobalMultiRaft.closed.Store(false)

	var err error
	GlobalMultiRaft.metaDir = config.GetRaftMetaPath()
	GlobalMultiRaft.raftMeta, err = OpenRaftMeta(GlobalMultiRaft.metaDir)
	if err != nil {
		return err
	}

	if err = GlobalMultiRaft.firstInitV8(c); err != nil {
		return err
	}

	GlobalMultiRaft.cleanSnapshotDir()

	rhc := &c.FiexedRaftNodeHost
	rsc := &c.RaftState
	rcc := &c.FiexedRaftCluster
	rcc.MaxInMemLogSize = DefaultMaxInMemLogSize
	defaultClusterConfig = rcc

	s.DoRaftSync = GlobalMultiRaft.Propose

	if err = initNodeHost(GlobalMultiRaft, rhc, rsc); err != nil {
		return err
	}

	GlobalMultiRaft.mu.Lock()
	GlobalMultiRaft.mu.clusterRaftGroup = make(map[uint64]*RaftNode, 1)
	GlobalMultiRaft.mu.Unlock()

	configList, err := loadNodeInitConfig()
	if err != nil {
		return err
	}

	if len(configList) > 0 {
		for _, cfg := range configList {
			if _, err = GlobalMultiRaft.StartRaftNode(cfg, false); err != nil {
				return err
			}
		}
	}

	GlobalMultiRaft.initRaftSlotGroup()
	GlobalMultiRaft.doRaftStatChange()
	return nil
}

func SetRaftNodeState(event string, v bool) {
	switch event {
	case cmd.ConfigSetRaftFullSync:
		order.G_NodeSates.SetFullSyncState(v)
	case cmd.ConfigSetRaftLogCompact:
		order.G_NodeSates.SetLogCompactState(v)
	default:
	}
}

func initNodeHost(mr *MultiRaft, rhc *config.FixedRaftNodeHostConfig, rsc *config.RaftStateConfig) error {
	mr.Nhc = dconfig.NodeHostConfig{
		WALDir:                        config.GetBitalosRaftWalPath(),
		NodeHostDir:                   config.GetBitalosRaftNodeHostPath(),
		RTTMillisecond:                rhc.Rtt,
		RaftAddress:                   rhc.RaftAddress,
		HostName:                      rhc.HostName,
		DeploymentID:                  rhc.DeploymentId,
		MaxSendQueueSize:              DefaultMaxSendQueueSize,
		MaxReceiveQueueSize:           DefaultMaxReceiveQueueSize,
		MaxSnapshotSendBytesPerSecond: uint64(rhc.MaxSnapshotSendBytesPerSecond.Int64()),
		MaxSnapshotRecvBytesPerSecond: uint64(rhc.MaxSnapshotRecvBytesPerSecond.Int64()),
	}
	mr.walDir = mr.Nhc.WALDir
	mr.nodehostDir = mr.Nhc.NodeHostDir

	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.ERROR)
	logger.GetLogger("transport").SetLevel(logger.ERROR)
	logger.GetLogger("grpc").SetLevel(logger.ERROR)
	logger.GetLogger("logdb").SetLevel(logger.ERROR)
	logger.GetLogger("raftpb").SetLevel(logger.ERROR)
	logger.GetLogger("bitalosraft").SetLevel(logger.ERROR)
	logger.GetLogger("dbconfig").SetLevel(logger.ERROR)
	logger.GetLogger("settings").SetLevel(logger.ERROR)
	logger.GetLogger("order").SetLevel(logger.ERROR)

	mr.Nhc.Expert.Engine = dconfig.GetDefaultEngineConfig()
	mr.Nhc.Expert.LogDB = dconfig.GetDefaultLogDBConfig()
	mr.Nhc.Expert.LogDB.Shards = 1
	mr.Nhc.Expert.Engine.ExecShards = 1
	mr.Nhc.Expert.Engine.CommitShards = 1
	mr.Nhc.Expert.Engine.ApplyShards = 1
	mr.Nhc.Expert.Engine.SnapshotShards = 1
	mr.Nhc.Expert.Engine.CloseShards = 1
	nodehost, err := bitalosraft.NewNodeHost(mr.Nhc)
	if err != nil {
		return err
	}

	mr.nodehost = nodehost
	nSelectTime := int64(DefaultElectionRTT * rhc.Rtt * uint64(time.Millisecond))
	updateInterval := rsc.Interval.Int64()
	allowMaxoffset := rsc.AllowMaxOffset
	order.G_NodeSates.SetPara(updateInterval, nSelectTime, allowMaxoffset)

	log.Infof("nodehost init success interval:%d rtt:%d config:%+v", updateInterval, nSelectTime, mr.Nhc)
	return nil
}

func (mr *MultiRaft) switchUpdateIndex() error {
	updateIndex := mr.s.GetMeta().GetUpdateIndex()
	if updateIndex == 0 {
		return nil
	}
	cid := mr.cfg.RaftCluster.ClusterId
	meta, err := OpenNodeMeta(mr.metaDir, cid)
	if err != nil {
		log.Errorf("open node meta err:%s", err)
		return err
	}
	meta.SetUpdateIndex(updateIndex)
	meta.Close()
	log.Infof("switch update index:%d to nodemeta cluster:%d", updateIndex, cid)
	return nil
}

func (mr *MultiRaft) StartRaftNode(initConfig *NodeInitConfig, firstStart bool) (*RaftNode, error) {
	if mr.closed.Load() {
		return nil, ErrRaftNotOpen
	}

	clusterId := initConfig.ClusterId
	v := mr.GetNodeByClusterId(clusterId)
	if v != nil {
		return nil, errors.New("cluster exists")
	}

	rn, err := NewRaftNode(initConfig, mr.nodehost, mr)
	if err != nil {
		return nil, err
	}

	if firstStart {
		if err = writeNodeInitConfig(initConfig); err != nil {
			log.Infof("write init node config err:%s", err)
		}
	}

	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.mu.clusterRaftGroup[clusterId] = rn
	return rn, nil
}

func (mr *MultiRaft) StopRaftNode(cid uint64, nodeId uint64) error {
	mr.mu.RLock()
	v, ok := mr.mu.clusterRaftGroup[cid]
	mr.mu.RUnlock()
	if !ok {
		return nil
	}

	// Error will be returned if stop observer node, and continue
	err := v.Nh.StopNode(cid, nodeId)
	if err != nil {
		log.Errorf("StopRaftNode StopNode cluster:%d node:%d err:%s", cid, nodeId, err)
	}

	v.CloseAndClear()
	log.Infof("StopRaftNode cluster:%d node:%d finish", cid, nodeId)

	mr.mu.Lock()
	delete(mr.mu.clusterRaftGroup, cid)
	mr.refreshOnlineCluster(true)
	mr.mu.Unlock()
	return nil
}

func (mr *MultiRaft) getOnlineNode() (*RaftNode, error) {
	mr.mu.RLock()
	defer mr.mu.RUnlock()
	if mr.mu.onlineClusterId == 0 {
		return nil, errors.New("not only one online cluster")
	}
	if v, ok := mr.mu.clusterRaftGroup[mr.mu.onlineClusterId]; !ok {
		return nil, errors.New("online node not exist")
	} else {
		return v, nil
	}
}

func (mr *MultiRaft) GetAnyOnlineNode() (*RaftNode, error) {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	for _, v := range mr.mu.clusterRaftGroup {
		if v.Online {
			return v, nil
		}
	}
	return nil, errors.New("no any online cluster")
}

func (mr *MultiRaft) GetNodeByClusterId(cid uint64) *RaftNode {
	mr.mu.RLock()
	defer mr.mu.RUnlock()
	if v, ok := mr.mu.clusterRaftGroup[cid]; !ok {
		return nil
	} else {
		return v
	}
}

func (mr *MultiRaft) IsOpen() bool {
	return mr.cfg.Plugin.OpenRaft && !mr.closed.Load()
}

func (mr *MultiRaft) isMaster() bool {
	if !mr.cfg.Plugin.OpenRaft || mr.cfg.CheckIsDegradeSingleNode() {
		return true
	}

	if !mr.IsOpen() {
		return false
	}

	num, _ := mr.getClusterNum()
	if num <= 0 {
		return false
	}

	mr.mu.RLock()
	defer mr.mu.RUnlock()
	for _, node := range mr.mu.clusterRaftGroup {
		if !node.Online {
			continue
		}
		if node.ClusterInfo.LeaderNodeId == node.NodeID {
			return true
		}
	}
	return false
}

// Set one raft online, set others offline
func (mr *MultiRaft) reserveOneGroupOnline(gid uint64) {
	for _, nd := range mr.mu.clusterRaftGroup {
		if nd.ClusterId != gid && nd.Online {
			nd.SwitchNodeOffline()
		}
	}
}

// Refresh the value of onlineClusterId.
// A: Call it when the number of clusters changes
// B: Call it when the online status of cluster changes
func (mr *MultiRaft) refreshOnlineCluster(hasLock bool) {
	onlineNum := 0
	onlineCid := uint64(0)
	if !hasLock {
		mr.mu.Lock()
		defer mr.mu.Unlock()
	}
	for _, nd := range mr.mu.clusterRaftGroup {
		if nd.Online {
			onlineNum++
			onlineCid = nd.ClusterId
		}
	}

	if onlineNum == 0 {
		mr.mu.onlineClusterId = 0
	} else if onlineNum == 1 {
		mr.mu.onlineClusterId = onlineCid
	} else {
		mr.mu.onlineClusterId = 0
	}
	log.Infof("onlineClusterNum:%d onlineClusterId:%d", onlineNum, mr.mu.onlineClusterId)
}

func (mr *MultiRaft) Propose(khash uint32, data [][]byte) ([]byte, error) {
	if mr.mapping.IsSpliting() {
		slotId := btools.GetSlotId(khash)
		clusterId := mr.mapping.GetGroup(slotId)
		if rn := mr.GetNodeByClusterId(clusterId); rn != nil {
			return rn.ProposeMessage(khash, data)
		} else {
			rn, err := mr.GetAnyOnlineNode()
			if err != nil {
				return nil, err
			} else {
				return rn.ProposeMessage(khash, data)
			}
		}
	} else {
		rn, err := mr.getOnlineNode()
		if err != nil {
			return nil, err
		}
		return rn.ProposeMessage(khash, data)
	}
}

func (mr *MultiRaft) SetOnlineGroups(groups map[uint64]bool) error {
	for gid, _ := range groups {
		if nd := mr.GetNodeByClusterId(gid); nd != nil {
			nd.SwitchNodeOnline()
		} else {
			return errors.New("cluster not found")
		}
	}
	return nil
}

// Init slot mapping, online cluster, refresh
func (mr *MultiRaft) initRaftSlotGroup() {
	meta := mr.raftMeta
	if meta.GetSlotInit() == SlotInitDone {
		num, _ := mr.getClusterNum()
		hasMultiCluster := num > 1
		mr.mapping = LoadSlotMapping(mr, meta, hasMultiCluster)
	} else {
		cid := mr.cfg.RaftCluster.ClusterId
		meta.InitAllSlots(uint16(cid))
		meta.SetSlotInit(SlotInitDone)
		mr.mapping = NewSlotMapping(mr, meta, cid)
		mr.SetOnlineGroups(map[uint64]bool{cid: true})
	}
	mr.s.SlotSpliting = mr.mapping.IsSpliting
	mr.refreshOnlineCluster(false)
}

func (mr *MultiRaft) getClusterNum() (num int, id uint64) {
	mr.mu.RLock()
	defer mr.mu.RUnlock()
	return len(mr.mu.clusterRaftGroup), mr.mu.onlineClusterId
}

func (mr *MultiRaft) cleanSnapshotDir() {
	os.RemoveAll(config.GetBitalosSnapshotPath())
	os.RemoveAll(config.GetBitalosRaftDbsyncPath())
}

func (mr *MultiRaft) deraftInit() {
	role := "single"
	status := false
	if mr.cfg.CheckIsDegradeSingleNode() {
		status = true
	}
	mr.deraftLock.Lock()
	defer mr.deraftLock.Unlock()
	mr.deraftInfo = &RaftInfo{
		ClusterId:     0,
		CurrentNodeId: 0,
		Role:          role,
		Status:        status,
		StartModel:    converRoleToStartModel(role),
	}
	mr.deraftInfo.UpdateCache()
}

func (mr *MultiRaft) deraftClean() {
	mr.deraftLock.Lock()
	defer mr.deraftLock.Unlock()
	mr.deraftInfo = nil
}

func (mr *MultiRaft) clear() error {
	mr.close()
	var err error
	if err = os.RemoveAll(mr.metaDir); err != nil {
		log.Errorf("remove %s err:%s", mr.metaDir, err)
		return err
	}
	if err = os.RemoveAll(nodeInitDir); err != nil {
		log.Errorf("remove %s err:%s", nodeInitDir, err)
		return err
	}
	if err = os.RemoveAll(mr.nodehostDir); err != nil {
		log.Errorf("remove %s err:%s", mr.nodehostDir, err)
		return err
	}
	if err = os.RemoveAll(mr.walDir); err != nil {
		log.Errorf("remove %s err:%s", mr.walDir, err)
		return err
	}
	return nil
}

func (mr *MultiRaft) close() {
	if !mr.IsOpen() {
		return
	}

	mr.closed.Store(true)
	mr.nodehost.Close()
	mr.queue.Close()

	mr.mu.Lock()
	defer mr.mu.Unlock()
	for _, rn := range mr.mu.clusterRaftGroup {
		rn.Close()
	}
	log.Infof("raft closed")
}
