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
	"context"
	"errors"
	"reflect"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/zuoyebang/bitalosraft"
	dconfig "github.com/zuoyebang/bitalosraft/config"
	"github.com/zuoyebang/bitalosraft/raftpb"
	"github.com/zuoyebang/bitalosraft/statemachine"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/marshal/update"
	"github.com/zuoyebang/bitalostored/stored/server"
	"google.golang.org/protobuf/proto"
)

type nodReqFun func(clusterID uint64, nodeID uint64, address string, configChangeIndex uint64,
	timeout time.Duration) (*bitalosraft.RequestState, error)

type RaftNode struct {
	ClusterId    uint64
	NodeID       uint64
	IsObserver   bool          // 对外展示的节点属性
	IsWitness    bool          // 对外展示的节点属性
	TimeOut      time.Duration // server需要的, propose/addnode/removenode超时时间
	ClusterInfo  *RaftInfo
	AsyncPropose bool
	RaftReady    bool
	Nh           *bitalosraft.NodeHost
	Rc           dconfig.Config
	Online       bool

	meta       *NodeMeta
	initConfig *NodeInitConfig
	queue      *Queue
	cfg        *config.Config
	metaDir    string
	disk       *DiskKV
	retryTimes int
}

func NewRaftNode(c *NodeInitConfig, nh *bitalosraft.NodeHost, mr *MultiRaft) (*RaftNode, error) {
	s := &RaftNode{
		Nh:          nh,
		ClusterInfo: NewRaftInfo(c.ClusterId, c.NodeID, mr.Nhc.RaftAddress, c.Role),
		initConfig:  c,
		queue:       mr.queue,
		cfg:         mr.cfg,
		retryTimes:  mr.cfg.RaftCluster.RetryTimes,
		metaDir:     mr.metaDir,
	}
	err := s.fillConfig(c.ClusterId, c.NodeID, c.Role, defaultClusterConfig)
	if err != nil {
		return nil, err
	}
	s.meta, err = OpenNodeMeta(mr.metaDir, c.ClusterId)
	if err != nil {
		return nil, err
	}
	err = s.startCluster(c.AddrList, c.NodeList, mr.s)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (rn *RaftNode) reloadMeta() error {
	meta, err := OpenNodeMeta(rn.metaDir, rn.ClusterId)
	if err != nil {
		log.Errorf("open meta err:%s cid:%d", err, rn.ClusterId)
		return err
	}
	rn.meta = meta
	return nil
}

func (rn *RaftNode) fillConfig(clusterId, nodeId uint64, role string, rcc *config.FixedRaftClusterConfig) error {
	rn.ClusterId = clusterId
	rn.NodeID = nodeId
	isObserver := false
	isWitness := false

	rn.TimeOut = time.Duration(rcc.TimeOut.Int64())
	rn.AsyncPropose = rcc.AsyncPropose

	switch role {
	case "observer":
		isObserver = true
	case "witness":
		isWitness = true
	case "normal":
	default:
		return errors.New("role is not available")
	}

	snapshotEntries := rcc.SnapshotEntries
	if isWitness {
		snapshotEntries = 0
	}
	rn.Rc = dconfig.Config{
		NodeID:                  rn.NodeID,
		ClusterID:               rn.ClusterId,
		ElectionRTT:             rcc.ElectionRTT,
		PreVote:                 true,
		HeartbeatRTT:            rcc.HeartbeatRTT,
		CheckQuorum:             rcc.CheckQuorum,
		SnapshotEntries:         snapshotEntries,
		CompactionOverhead:      rcc.CompactionOverhead,
		MaxInMemLogSize:         rcc.MaxInMemLogSize,
		SnapshotCompressionType: raftpb.CompressionType(rcc.SnapshotCompressionType),
		EntryCompressionType:    raftpb.CompressionType(rcc.EntryCompressionType),
		DisableAutoCompactions:  rcc.DisableAutoCompactions,
		IsObserver:              isObserver,
		IsWitness:               isWitness,
	}
	log.Infof("raft node fill config. node config:%+v", rn)
	return nil
}

func (rn *RaftNode) checkpoint(dstDir string) error {
	return rn.meta.Checkpoint(dstDir)
}

func (rn *RaftNode) startCluster(addrList []string, nodeList []uint64, s *server.Server) error {
	if len(addrList) != len(nodeList) {
		return errors.New("addrList and nodeList must have same length")
	}

	addrMap := make(map[uint64]string, len(addrList))
	for i, addr := range addrList {
		addrMap[nodeList[i]] = addr
	}

	if err := rn.Nh.StartOnDiskCluster(addrMap, false, func(clusterID uint64, nodeID uint64) statemachine.IOnDiskStateMachine {
		disk := NewDiskKV(clusterID, nodeID, s, rn)
		rn.disk = disk.(*DiskKV)
		return disk
	}, rn.Rc); err != nil {
		log.Fatalf("raft startOnDiskCluster fail err:%v clusterId:%d nodeId:%d initMember:%+v", err, rn.Rc.ClusterID, rn.Rc.NodeID, addrMap)
		return err
	}
	log.Infof("raft startOnDiskCluster success clusterId:%d nodeId:%d initMembers:%+v", rn.Rc.ClusterID, rn.Rc.NodeID, addrMap)
	rn.RaftReady = true
	return nil
}

func (rn *RaftNode) ProposeMessage(keyHash uint32, data [][]byte) ([]byte, error) {
	migrate := false
	b, err := proto.Marshal(&update.ByteSlice{
		IsMigrate: &migrate,
		NodeId:    &rn.NodeID,
		Data:      data,
		KeyHash:   &keyHash,
	})
	if err != nil {
		return nil, err
	}

	n := 0
	if rn.AsyncPropose {
		_, err = rn.doPropose(b, &n, rn.retryTimes)
		return nil, err
	} else {
		res, err := rn.SyncPropose(b)
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

func (rn *RaftNode) SwitchNodeOnline() error {
	if rn.Online {
		return nil
	}

	rn.Online = true
	return nil
}

func (rn *RaftNode) SwitchNodeOffline() error {
	if !rn.Online {
		return nil
	}

	rn.Online = false
	return nil
}

func (rn *RaftNode) SyncPropose(msg []byte) (statemachine.Result, error) {
	if !rn.RaftReady {
		return statemachine.Result{}, errn.ErrRaftNotReady
	}
	ctx, cancel := context.WithTimeout(context.Background(), rn.TimeOut)
	res, err := rn.Nh.SyncPropose(ctx, rn.Nh.GetNoOPSession(rn.Rc.ClusterID), msg)
	cancel()
	return res, err
}

func (rn *RaftNode) doPropose(msg []byte, n *int, max int) (RetType, error) {
	if *n >= max {
		return R_RETRY_EXHAUST, errors.New("the retry number has been reached")
	}

	*n = *n + 1

	if _, err := rn.Nh.Propose(rn.Nh.GetNoOPSession(rn.Rc.ClusterID), msg, rn.TimeOut); err != nil {
		return R_ERROR, err
	}

	return R_SUCCESS, nil
}

func (rn *RaftNode) IsMaster() bool {
	if rn.NodeID > 0 {
		leaderId, ok, err := rn.Nh.GetLeaderID(rn.ClusterId)
		if err == nil && ok && leaderId == rn.NodeID {
			return true
		}
	}
	return false
}

func (rn *RaftNode) rewriteConfig() error {
	log.Infof("write config:%+v", *rn.initConfig)
	if err := writeNodeInitConfig(rn.initConfig); err != nil {
		log.Errorf("write init config err. config:%+v err:%s", rn.initConfig, err)
		return err
	}
	return nil
}

func (rn *RaftNode) Close() {
	if rn.disk != nil {
		rn.disk.Close()
	}
	rn.meta.Close()
}

func (rn *RaftNode) CloseAndClear() {
	rn.Close()
	if err := removeNodeInitConfig(rn.ClusterId); err != nil {
		log.Errorf("remove node init config cluster:%d err:%s", rn.ClusterId, err)
	}
	if _, err := rn.RemoveData(rn.NodeID); err != nil {
		log.Errorf("remove node data fail cluster:%d node:%d err:%s", rn.ClusterId, rn.NodeID, err)
	} else {
		log.Infof("remove node data success cluster:%d node:%d", rn.ClusterId, rn.NodeID)
	}
	if err := rn.meta.Remove(); err != nil {
		log.Errorf("remove node meta cluster:%d err:%s", rn.ClusterId, err)
	}
}

func (rn *RaftNode) DelNode(nodeId uint64, retryTimes int) (RetType, error) {
	if !rn.RaftReady {
		err := " raft is not ok "
		log.Error(err)
		return R_NIL_POINTER, errors.New(err)
	}

	if retryTimes < 1 {
		err := " retryTimes is too small "
		log.Error(err)
		return R_PARA_ERR, errors.New(err)
	}

	var n int = 0
	return rn.doDelNode(nodeId, &n, retryTimes)
}

func (rn *RaftNode) doDelNode(nodeId uint64, n *int, max int) (RetType, error) {
	if *n >= max {
		err := " the number of retries has been reached"
		log.Error(err)
		return R_RETRY_EXHAUST, errors.New(err)
	}

	*n = *n + 1

	rs, err := rn.Nh.RequestDeleteNode(rn.Rc.ClusterID, nodeId, 0, rn.TimeOut)
	if nil != err {
		log.Error("DelNode ", err)
		return R_ERROR, err
	}

	s := <-rs.CompletedC
	if s.Timeout() || s.Dropped() {
		log.Warn(" retry, timeout: ", s.Timeout(), " dropped : ", s.Dropped())
		rn.doDelNode(nodeId, n, max)
	} else if s.Completed() {
		rs.Release()
	} else if s.Terminated() {
		err := "should be shutdown"
		log.Error(err)
		return R_SHUT_DOWN, errors.New(err)
	} else if s.Rejected() {
		err := "Rejected"
		log.Error(err)
		return R_REJECTED, errors.New(err)
	} else if s.Aborted() {
		return R_ABORTED, errors.New("aborted")
	} else {
		return R_UNKNOWN_ERROR, errors.New("unknown err")
	}

	return R_SUCCESS, nil
}

func (rn *RaftNode) GetNodeHostInfo() (string, RetType, error) {
	if !rn.RaftReady {
		err := " raft is not ok "
		log.Error(err)
		return "", R_NIL_POINTER, errors.New(err)
	}
	var opt bitalosraft.NodeHostInfoOption
	pRet := rn.Nh.GetNodeHostInfo(opt)
	if nil == pRet {
		return "", R_RET_ERROR, errors.New(" return nil pointer")
	}

	var out NodeHostInfoV2
	out.Info = *pRet
	data, err := jsoniter.Marshal(out)
	if nil != err {
		return "", R_JOSON_ERR, err
	}
	return unsafe2.String(data), R_SUCCESS, nil
}

func (rn *RaftNode) GetClusterMembership() (*MembershipV2, RetType, error) {
	if !rn.RaftReady {
		return nil, R_NIL_POINTER, ErrRaftNotReady
	}
	ctx, cancel := context.WithTimeout(context.Background(), rn.TimeOut)
	defer cancel()

	membership, err := rn.Nh.SyncGetClusterMembership(ctx, rn.Rc.ClusterID)
	if nil != err {
		return nil, R_RET_ERROR, err
	}

	m := &MembershipV2{}
	m.InitByDragonboatMembership(membership)
	return m, R_SUCCESS, nil
}

func (rn *RaftNode) LeaderTransfer(targetNodeID uint64) (RetType, error) {
	if !rn.RaftReady {
		return R_NIL_POINTER, ErrRaftNotReady
	}

	err := rn.Nh.RequestLeaderTransfer(rn.Rc.ClusterID, targetNodeID)
	if nil != err {
		return R_ERROR, err
	}
	return R_SUCCESS, nil
}

func (rn *RaftNode) RemoveData(nNodeID uint64) (RetType, error) {
	if !rn.RaftReady {
		return R_NIL_POINTER, ErrRaftNotReady
	}

	if err := rn.Nh.RemoveData(rn.Rc.ClusterID, nNodeID); err != nil {
		return R_ERROR, err
	}

	return R_SUCCESS, nil
}

func (rn *RaftNode) StopNodeHost() (RetType, error) {
	if !rn.RaftReady {
		return R_NIL_POINTER, ErrRaftNotReady
	}
	rn.RaftReady = false
	rn.Nh.Close()
	return R_SUCCESS, nil
}

func (rn *RaftNode) AddObserver(nodeId uint64, address string, retry int) (RetType, error) {
	return rn.addRaftNode(nodeId, address, retry, rn.Nh.RequestAddNonVoting)
}

func (rn *RaftNode) AddWitness(nodeId uint64, address string, retry int) (RetType, error) {
	return rn.addRaftNode(nodeId, address, retry, rn.Nh.RequestAddWitness)
}

func (rn *RaftNode) AddNode(nodeId uint64, address string, retryTimes int) (RetType, error) {
	return rn.addRaftNode(nodeId, address, retryTimes, rn.Nh.RequestAddNode)
}

func (rn *RaftNode) addRaftNode(nodeId uint64, address string, retryTimes int, addReq nodReqFun) (RetType, error) {
	if !rn.RaftReady {
		err := " raft is not ok "
		log.Error(err)
		return R_NIL_POINTER, errors.New(err)
	}

	if retryTimes < 1 {
		err := " retryTimes is too small "
		log.Error(err)
		return R_PARA_ERR, errors.New(err)
	}

	var n int = 0
	return rn.doAddRaftNode(nodeId, address, &n, retryTimes, addReq)
}

func (rn *RaftNode) doAddRaftNode(nodeId uint64, address string, n *int, max int, addReq nodReqFun) (RetType, error) {
	if *n >= max {
		err := " the number of retries has been reached"
		log.Error(err)
		return R_RETRY_EXHAUST, errors.New(err)
	}

	*n = *n + 1

	rs, err := addReq(rn.Rc.ClusterID, nodeId, address, 0, rn.TimeOut)
	if nil != err {
		log.Error("AddNode ", reflect.ValueOf(addReq).String(), " err: ", err)
		return R_ERROR, err
	}
	s := <-rs.CompletedC
	if s.Timeout() || s.Dropped() {
		log.Warn(" retry, timeout: ", s.Timeout(), " dropped : ", s.Dropped())
		rn.doAddRaftNode(nodeId, address, n, max, addReq)
	} else if s.Completed() {
		rs.Release()
	} else if s.Terminated() {
		err := "should be shutdown"
		log.Error(err)
		return R_SHUT_DOWN, errors.New(err)
	} else if s.Rejected() {
		err := " Rejected"
		log.Error(err)
		return R_REJECTED, errors.New(err)
	} else if s.Aborted() {
		err := "aborted"
		return R_ABORTED, errors.New(err)
	} else {
		return R_UNKNOWN_ERROR, errors.New(" unknown err")
	}
	return R_SUCCESS, nil
}
