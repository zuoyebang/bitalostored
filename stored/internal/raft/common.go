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
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	braft "github.com/zuoyebang/bitalostored/raft"
	dconfig "github.com/zuoyebang/bitalostored/raft/config"
	"github.com/zuoyebang/bitalostored/raft/logger"
	"github.com/zuoyebang/bitalostored/raft/order"
	"github.com/zuoyebang/bitalostored/raft/statemachine"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/marshal/update"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
	"github.com/zuoyebang/bitalostored/stored/server"
	"google.golang.org/protobuf/proto"
)

var raftInstance = &StartRun{}

type StartRun struct {
	ClusterId       uint64
	NodeID          uint64
	Addr            string
	Join            bool
	IsObserver      bool
	IsWitness       bool
	TimeOut         time.Duration
	RetryTimes      int
	SnapshotTimeOut int
	AsyncPropose    bool
	RaftReady       bool
	Mu              sync.Mutex
	ClusterStatOnce sync.Once
	AddrList        map[uint64]string
	WalDir          string
	NodeHostDir     string
	HostName        string
	Nhc             dconfig.NodeHostConfig
	Nh              *braft.NodeHost
	Rc              dconfig.Config

	queue         *Queue
	bStopNodeHost bool
}

func (p *StartRun) LoadConfig(s *server.Server) {
	p.ClusterId = config.GlobalConfig.RaftCluster.ClusterId
	p.NodeID = config.GlobalConfig.RaftNodeHost.NodeID
	p.Addr = config.GlobalConfig.RaftNodeHost.RaftAddress
	p.Join = config.GlobalConfig.RaftCluster.Join
	p.HostName = config.GlobalConfig.RaftNodeHost.HostName

	p.WalDir = config.GetBitalosRaftWalPath()
	p.NodeHostDir = config.GetBitalosRaftNodeHostPath()

	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.ERROR)
	logger.GetLogger("transport").SetLevel(logger.ERROR)
	logger.GetLogger("grpc").SetLevel(logger.ERROR)
	logger.GetLogger("logdb").SetLevel(logger.ERROR)
	logger.GetLogger("raftpb").SetLevel(logger.ERROR)
	logger.GetLogger("dragonboat").SetLevel(logger.ERROR)
	logger.GetLogger("dbconfig").SetLevel(logger.ERROR)
	logger.GetLogger("settings").SetLevel(logger.ERROR)
	logger.GetLogger("order").SetLevel(logger.ERROR)

	p.TimeOut = time.Duration(config.GlobalConfig.RaftCluster.TimeOut.Int64())
	p.RetryTimes = config.GlobalConfig.RaftCluster.RetryTimes
	p.AsyncPropose = config.GlobalConfig.RaftCluster.AsyncPropose
	p.SnapshotTimeOut = int(config.GlobalConfig.RaftNodeHost.SnapshotTimeout.Int64())

	p.AddrList = make(map[uint64]string)
	p.transInitClusterMem()
	nodeAddr := p.getAddr()

	maxSendQueueSize := uint64(256 << 20)
	maxReceiveQueueSize := uint64(256 << 20)
	p.Nhc = dconfig.NodeHostConfig{
		WALDir:                        p.WalDir,
		NodeHostDir:                   p.NodeHostDir,
		RTTMillisecond:                config.GlobalConfig.RaftNodeHost.Rtt,
		RaftAddress:                   nodeAddr,
		HostName:                      p.HostName,
		DeploymentID:                  config.GlobalConfig.RaftNodeHost.DeploymentId,
		MaxSendQueueSize:              maxSendQueueSize,
		MaxReceiveQueueSize:           maxReceiveQueueSize,
		MaxSnapshotSendBytesPerSecond: uint64(config.GlobalConfig.RaftNodeHost.MaxSnapshotSendBytesPerSecond.Int64()),
		MaxSnapshotRecvBytesPerSecond: uint64(config.GlobalConfig.RaftNodeHost.MaxSnapshotRecvBytesPerSecond.Int64()),
	}

	p.Nhc.Expert.LogDB = dconfig.GetDefaultLogDBConfig()
	p.Nhc.Expert.Engine = dconfig.GetDefaultEngineConfig()

	p.Nhc.Expert.LogDB.Shards = 1
	p.Nhc.Expert.LogDB.KVWriteBufferSize = 128 << 20
	p.Nhc.Expert.LogDB.KVTargetFileSizeBase = 128 << 20
	p.Nhc.Expert.Engine.ExecShards = 1
	p.Nhc.Expert.Engine.CommitShards = 1
	p.Nhc.Expert.Engine.ApplyShards = 1
	p.Nhc.Expert.Engine.SnapshotShards = 1
	p.Nhc.Expert.Engine.CloseShards = 1

	var flushCallback func(uint64)
	if !s.IsWitness {
		flushCallback = s.FlushCallback
	}
	maxInMemLogSize := uint64(1 * 1073741824)
	p.Rc = dconfig.Config{
		NodeID:                  p.NodeID,
		ClusterID:               p.ClusterId,
		ElectionRTT:             config.GlobalConfig.RaftCluster.ElectionRTT,
		PreVote:                 true,
		HeartbeatRTT:            config.GlobalConfig.RaftCluster.HeartbeatRTT,
		CheckQuorum:             config.GlobalConfig.RaftCluster.CheckQuorum,
		SnapshotEntries:         config.GlobalConfig.RaftCluster.SnapshotEntries,
		CompactionOverhead:      config.GlobalConfig.RaftCluster.CompactionOverhead,
		MaxInMemLogSize:         maxInMemLogSize,
		SnapshotCompressionType: dconfig.CompressionType(config.GlobalConfig.RaftCluster.SnapshotCompressionType),
		EntryCompressionType:    dconfig.CompressionType(config.GlobalConfig.RaftCluster.EntryCompressionType),
		DisableAutoCompactions:  config.GlobalConfig.RaftCluster.DisableAutoCompactions,
		IsObserver:              config.GlobalConfig.RaftCluster.IsObserver,
		IsWitness:               config.GlobalConfig.RaftCluster.IsWitness,
		FlushCallback:           flushCallback,
	}

	nSelectTime := int64(p.Rc.ElectionRTT * p.Nhc.RTTMillisecond * uint64(time.Millisecond))
	updateInterval := config.GlobalConfig.RaftState.Internal.Int64()
	allowMaxOffset := config.GlobalConfig.RaftState.AllowMaxOffset
	order.G_NodeSates.SetPara(updateInterval, nSelectTime, allowMaxOffset)

	log.Infof("create logdb success dumpRaftConfig[%s]", p.dumpRaftConfig())
}

func (p *StartRun) dumpRaftConfig() string {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "LogDBShards:%d ", p.Nhc.Expert.LogDB.Shards)
	fmt.Fprintf(&buf, "ExecShards:%d ", p.Nhc.Expert.Engine.ExecShards)
	fmt.Fprintf(&buf, "RTTMillisecond:%d ", p.Nhc.RTTMillisecond)
	fmt.Fprintf(&buf, "RaftAddress:%s ", p.Nhc.RaftAddress)
	fmt.Fprintf(&buf, "DeploymentID:%d ", p.Nhc.DeploymentID)
	fmt.Fprintf(&buf, "MaxSendQueueSize:%d ", p.Nhc.MaxSendQueueSize)
	fmt.Fprintf(&buf, "MaxSnapshotSendBytesPerSecond:%d ", p.Nhc.MaxSnapshotSendBytesPerSecond)
	fmt.Fprintf(&buf, "MaxSnapshotRecvBytesPerSecond:%d ", p.Nhc.MaxSnapshotRecvBytesPerSecond)

	fmt.Fprintf(&buf, "NodeID:%d ", p.Rc.NodeID)
	fmt.Fprintf(&buf, "ClusterI:%d ", p.Rc.ClusterID)
	fmt.Fprintf(&buf, "ElectionRTT:%d ", p.Rc.ElectionRTT)
	fmt.Fprintf(&buf, "HeartbeatRTT:%d ", p.Rc.HeartbeatRTT)
	fmt.Fprintf(&buf, "CheckQuorum:%v ", p.Rc.CheckQuorum)
	fmt.Fprintf(&buf, "SnapshotEntries:%d ", p.Rc.SnapshotEntries)
	fmt.Fprintf(&buf, "CompactionOverhead:%d ", p.Rc.CompactionOverhead)
	fmt.Fprintf(&buf, "MaxInMemLogSize:%d ", p.Rc.MaxInMemLogSize)
	fmt.Fprintf(&buf, "SnapshotCompressionType:%d ", p.Rc.SnapshotCompressionType)
	fmt.Fprintf(&buf, "EntryCompressionType:%d ", p.Rc.EntryCompressionType)
	fmt.Fprintf(&buf, "DisableAutoCompactions:%v ", p.Rc.DisableAutoCompactions)

	return buf.String()
}

func (p *StartRun) Clean() error {
	if err := os.RemoveAll(p.NodeHostDir); err != nil {
		log.Errorf("remove %s err:%v", p.NodeHostDir, err)
		return err
	} else {
		log.Infof("remove %s succ", p.NodeHostDir)
	}

	if err := os.RemoveAll(p.WalDir); err != nil {
		log.Errorf("remove %s err:%v", p.WalDir, err)
		return err
	} else {
		log.Infof("remove %s succ", p.WalDir)
	}

	snapshotDir := config.GetBitalosSnapshotPath()
	if err := os.RemoveAll(snapshotDir); err != nil {
		log.Errorf("remove snapshot path:%s err:%v", snapshotDir, err)
		return err
	} else {
		log.Infof("remove snapshot path:%s succ", snapshotDir)
	}
	return nil
}

func (p *StartRun) transInitClusterMem() {
	if p.Join {
		return
	}

	list := strings.Split(os.Getenv("RAFT_ADDR"), ",")

	if len(list) < 3 {
		list = config.GlobalConfig.RaftNodeHost.InitRaftAddrList
	}

	if len(list) <= 0 {
		list = []string{
			config.GlobalConfig.RaftNodeHost.RaftAddress,
		}
	}
	if nil == p.AddrList {
		p.AddrList = make(map[uint64]string)
	}
	if len(config.GlobalConfig.RaftNodeHost.InitRaftNodeList) > 0 {
		if len(config.GlobalConfig.RaftNodeHost.InitRaftNodeList) != len(list) {
			panic("init_raft_addrlist not match init_raft_nodelist")
		}
		nodelist := config.GlobalConfig.RaftNodeHost.InitRaftNodeList
		for idx, v := range list {
			p.AddrList[nodelist[idx]] = v
		}
	} else {
		for idx, v := range list {
			p.AddrList[uint64(idx+1)] = v
		}
	}
	log.Info("addlist: ", p.AddrList)
}

func (p *StartRun) validateNodeId() bool {
	if !(config.GlobalConfig.RaftCluster.Join || config.GlobalConfig.RaftCluster.IsObserver ||
		config.GlobalConfig.RaftCluster.IsWitness) &&
		(p.NodeID > (uint64(len(p.AddrList))) || (p.NodeID <= 0)) {
		log.Warn("the Node id wrong can't equal to 0 or greater than  ", len(p.AddrList), " node id :", p.NodeID)
		return false
	}
	return true
}

func (p *StartRun) getAddr() string {
	if len(p.Addr) <= 0 {
		p.Addr = p.AddrList[p.NodeID]
	}

	return p.Addr
}

func (p *StartRun) Start(s *server.Server) {
	p.LoadConfig(s)

	p.registerRaftCommand(s)
	p.registerIsMasterCF(s)
	p.registerSyncToSlave(s)

	if !config.GlobalConfig.Plugin.OpenRaft || config.GlobalConfig.CheckIsDegradeSingleNode() {
		s.Info.Cluster.Role = "single"
		s.Info.Cluster.Status = true
	} else {
		node, err := braft.NewNodeHost(p.Nhc)
		if err != nil {
			log.Error("new host: ", err)
			panic(err)
		}
		p.Nh = node

		if err := p.Nh.StartOnDiskCluster(p.AddrList, p.Join, func(clusterID uint64, nodeID uint64) statemachine.IOnDiskStateMachine {
			s.Info.Cluster.ClusterId = clusterID
			s.Info.Cluster.CurrentNodeId = nodeID
			return NewDiskKV(clusterID, nodeID, s, p)
		}, p.Rc); err != nil {
			log.Error("start cluster: ", err)
			panic(err)
		}
		p.RaftReady = true
	}

	p.doRaftClusterStat(s)
}

func (p *StartRun) Stop() {
	if p != nil && p.Nh != nil {
		p.StopNodeHost()
	}
}

func (p *StartRun) Sync(keyHash uint32, data [][]byte) ([]byte, error) {
	migrate := false

	b, err := proto.Marshal(&update.ByteSlice{
		IsMigrate: &migrate,
		NodeId:    &p.NodeID,
		Data:      data,
		KeyHash:   &keyHash,
	})
	if err != nil {
		return nil, err
	}

	if p.AsyncPropose {
		_, err = p.Propose(b, p.RetryTimes)
		return nil, err
	} else {
		res, err := p.SyncPropose(b)
		if err != nil {
			return nil, err
		}

		if bytes.Equal(res.Data, UpdateSelfNodeDoing) {
			return nil, nil
		} else {
			return res.Data, nil
		}
	}
}

func GetClusterNodeOK(nCluster uint64) bool {
	return order.G_NodeSates.OK(nCluster)
}

func RaftInit(s *server.Server) {
	logger.SetLoggerFactory(func(name string) logger.ILogger {
		return DefaultLogger
	})

	s.DoRaftSync = raftInstance.Sync
	s.DoRaftStop = raftInstance.Stop
}

func RaftStart(s *server.Server) {
	raftInstance.Start(s)
}

func ReraftInit(s *server.Server, port string) error {
	config.GlobalConfig.RaftNodeHost.RaftAddress = utils.GetLocalIp() + ":" + port
	config.GlobalConfig.RaftNodeHost.InitRaftAddrList = []string{config.GlobalConfig.RaftNodeHost.RaftAddress}
	config.GlobalConfig.RaftNodeHost.InitRaftNodeList = []uint64{config.GlobalConfig.RaftNodeHost.NodeID}

	raftInstance.LoadConfig(s)

	node, err := braft.NewNodeHost(raftInstance.Nhc)
	if err != nil {
		log.Errorf("new node host fail err:%v", err)
		return err
	}
	raftInstance.Nh = node

	if err = raftInstance.Nh.StartOnDiskCluster(raftInstance.AddrList, raftInstance.Join, func(clusterID uint64, nodeID uint64) statemachine.IOnDiskStateMachine {
		s.Info.Cluster.ClusterId = clusterID
		s.Info.Cluster.CurrentNodeId = nodeID
		config.GlobalConfig.Plugin.OpenRaft = true
		config.GlobalConfig.Server.DegradeSingleNode = false
		return NewDiskKV(clusterID, nodeID, s, raftInstance)
	}, raftInstance.Rc); err != nil {
		log.Errorf("start cluster fail err:%v", err)
		return err
	}
	raftInstance.RaftReady = true
	return nil
}
