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
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zuoyebang/bitalosraft"
	"github.com/zuoyebang/bitalosraft/order"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/internal/bytepools"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/trycatch"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

type RaftInfo struct {
	StartModel    ModelType `json:"start_model"`     //节点启动模式
	ClusterId     uint64    `json:"cluster_id"`      //raft集群
	CurrentNodeId uint64    `json:"current_node_id"` //节点ID
	Role          string    `json:"role"`            //节点角色
	Status        bool      `json:"status"`          //节点可用状态
	Online        bool      `json:"online"`          //节点是否可用
	RaftAddress   string    `json:"raft_address"`    //raft地址

	LeaderNodeId     uint64 `json:"leader_node_id"`     //当前主节点ID
	LeaderAddress    string `json:"leader_address"`     //当前主节点地址
	ClusterNodes     string `json:"cluster_nodes"`      //集群节点ID列表
	ClusterNodesList string `json:"cluster_nodes_list"` //集群节点列表
	RaftLogIndex     uint64 `json:"raft_log_index"`

	mutex sync.RWMutex
	cache []byte
}

func NewRaftInfo(clusterId uint64, nodeId uint64, addr string, role string) *RaftInfo {
	return &RaftInfo{
		ClusterId:     clusterId,
		CurrentNodeId: nodeId,
		RaftAddress:   addr,
		cache:         make([]byte, 0, 2048),
		StartModel:    converRoleToStartModel(role),
	}
}

func (ri *RaftInfo) Marshal() ([]byte, func()) {
	ri.mutex.RLock()
	defer ri.mutex.RUnlock()

	info, closer := bytepools.GlobalBytePools.GetBytePool(len(ri.cache))
	num := copy(info[0:], ri.cache)
	return info[:num], closer
}

func (ri *RaftInfo) ClearCache() {
	ri.mutex.Lock()
	defer ri.mutex.Unlock()

	ri.Role = "none"
	ri.Status = false
	ri.UpdateCache()
}

func (ri *RaftInfo) UpdateCache() {
	ri.mutex.Lock()
	defer ri.mutex.Unlock()

	ri.cache = ri.cache[:0]

	ri.cache = append(ri.cache, []byte("# ClusterInfo\n")...)
	ri.cache = utils.AppendInfoString(ri.cache, "start_model:", ri.StartModel.String())
	ri.cache = utils.AppendInfoString(ri.cache, "status:", utils.BoolToString(ri.Status))
	ri.cache = utils.AppendInfoString(ri.cache, "online:", utils.BoolToString(ri.Online))
	ri.cache = utils.AppendInfoString(ri.cache, "role:", ri.Role)
	ri.cache = utils.AppendInfoUint(ri.cache, "cluster_id:", ri.ClusterId)
	ri.cache = utils.AppendInfoUint(ri.cache, "current_node_id:", ri.CurrentNodeId)
	ri.cache = utils.AppendInfoString(ri.cache, "raft_address:", ri.RaftAddress)
	ri.cache = utils.AppendInfoUint(ri.cache, "leader_node_id:", ri.LeaderNodeId)
	ri.cache = utils.AppendInfoString(ri.cache, "leader_address:", ri.LeaderAddress)
	ri.cache = utils.AppendInfoString(ri.cache, "cluster_nodes:", ri.ClusterNodes)
	ri.cache = utils.AppendInfoUint(ri.cache, "raft_log_index:", ri.RaftLogIndex)
	ri.cache = append(ri.cache, ri.ClusterNodesList...)

	ri.cache = append(ri.cache, '\n')
}

func (mr *MultiRaft) GetAllRaftInfo() ([]byte, []func(), error) {
	mr.deraftLock.RLock()
	defer mr.deraftLock.RUnlock()
	res := make(map[uint64]string, 0)
	closers := make([]func(), 0)
	if mr.deraftInfo != nil {
		n, closer := mr.deraftInfo.Marshal()
		res[mr.deraftInfo.ClusterId] = unsafe2.String(n)
		if closer != nil {
			closers = append(closers, closer)
		}
		info, err := json.Marshal(res)
		return info, closers, err
	}

	if !mr.IsOpen() {
		return nil, nil, nil
	}

	var nodeInfo []byte
	var closer func()
	mr.mu.RLock()
	if len(mr.mu.clusterRaftGroup) == 0 {
		mr.mu.RUnlock()
		return nil, nil, nil
	}
	for cid, n := range mr.mu.clusterRaftGroup {
		nodeInfo, closer = n.ClusterInfo.Marshal()
		res[cid] = unsafe2.String(nodeInfo)
		if closer != nil {
			closers = append(closers, closer)
		}
	}
	mr.mu.RUnlock()
	info, err := json.Marshal(res)
	return info, closers, err
}

func (mr *MultiRaft) GetRaftInfo(clusterId uint64) ([]byte, func()) {
	mr.deraftLock.RLock()
	defer mr.deraftLock.RUnlock()
	if mr.deraftInfo != nil {
		return mr.deraftInfo.Marshal()
	}

	if !mr.IsOpen() {
		return nil, nil
	}
	mr.mu.RLock()
	defer mr.mu.RUnlock()
	if n, ok := mr.mu.clusterRaftGroup[clusterId]; ok {
		return n.ClusterInfo.Marshal()
	} else {
		return nil, nil
	}
}

func (mr *MultiRaft) doRaftStatChange() {
	if mr.statRunning {
		return
	}
	mr.statRunning = true

	mr.refreshOnce.Do(func() {
		go func() {
			mr.refreshBuf = bytes.NewBuffer(make([]byte, 0, 128))
			mr.refreshNodes = make([]string, 0, 5)

			for {
				mr.refreshRaftInfo()
				time.Sleep(8 * time.Second)
			}
		}()
	})
}

func (mr *MultiRaft) refreshRaftInfo() {
	defer func() {
		trycatch.Panic("refreshRaftInfo", recover())
	}()

	if mr == nil {
		return
	}

	if mr.nodehost == nil || mr.closed.Load() {
		if mr.s.Info.Stats.QueueLen != 0 {
			mr.s.Info.Stats.QueueLen = 0
			mr.s.Info.Stats.UpdateCache()
		}
		return
	}

	if mr.queue != nil {
		mr.s.Info.Stats.QueueLen = mr.queue.QLength()
		mr.s.Info.Stats.UpdateCache()
	}

	var opt bitalosraft.NodeHostInfoOption
	opt.SkipLogInfo = true
	res := mr.nodehost.GetNodeHostInfo(opt)
	if res == nil {
		return
	}

	onlineNodes := make(map[uint64]struct{}, len(res.ClusterInfoList))
	for _, raftCluster := range res.ClusterInfoList {
		cid := raftCluster.ClusterID
		node := mr.GetNodeByClusterId(cid)
		if node == nil {
			continue
		}

		nodeInfo := node.ClusterInfo
		nodeInfo.RaftLogIndex = node.meta.GetUpdateIndex()
		if nodeInfo.ClusterId != cid || nodeInfo.CurrentNodeId != raftCluster.NodeID {
			log.Errorf("cluster or node id not match. raftCluster:%d memCluster:%d, raftNode:%d memNode:%d",
				cid, nodeInfo.ClusterId, raftCluster.NodeID, nodeInfo.CurrentNodeId)
			nodeInfo.ClearCache()
			continue
		}

		// role 表示已经加入到集群的状态
		if raftCluster.IsLeader {
			nodeInfo.Role = "master"
		} else if raftCluster.IsNonVoting {
			nodeInfo.Role = "observer"
			node.IsObserver = true
		} else if raftCluster.IsWitness {
			nodeInfo.Role = "witness"
			node.IsWitness = true
		} else {
			nodeInfo.Role = "slave"
		}

		nodeInfo.Status = getClusterNodeOK(cid)
		if nodeInfo.Status {
			onlineNodes[raftCluster.ClusterID] = struct{}{}
		}

		if node.IsObserver {
			nodeInfo.StartModel = M_OBSERVER
			// observer raft状态ok 且更新为投票节点后 则该节点变成slave
			if nodeInfo.Status && !raftCluster.IsNonVoting {
				node.initConfig.Role = "normal"
				node.rewriteConfig()
				node.IsObserver = false
				nodeInfo.StartModel = M_NORMAL
			}
		}
		if node.IsWitness {
			nodeInfo.StartModel = M_WITNESS
		}

		if leaderNodeId, ok, err := mr.nodehost.GetLeaderID(raftCluster.ClusterID); ok && err == nil {
			nodeInfo.LeaderNodeId = leaderNodeId
			nodeInfo.LeaderAddress = raftCluster.Nodes[leaderNodeId]
		}

		mr.refreshNodes = mr.refreshNodes[:0]
		for i := range raftCluster.Nodes {
			mr.refreshNodes = append(mr.refreshNodes, strconv.FormatInt(int64(i), 10))
		}
		sort.Strings(mr.refreshNodes)
		nodeInfo.ClusterNodes = strings.Join(mr.refreshNodes, ",")
		for _, index := range mr.refreshNodes {
			if nodeId, err := strconv.ParseInt(index, 10, 64); err == nil {
				fmt.Fprintf(mr.refreshBuf, "node_%d:%s,state=online,node_id=%d\n", nodeId, raftCluster.Nodes[uint64(nodeId)], nodeId)
			}
		}
		nodeInfo.ClusterNodesList = mr.refreshBuf.String()
		mr.refreshBuf.Reset()

		nodeInfo.UpdateCache()
	}

	mr.mu.RLock()
	defer mr.mu.RUnlock()
	for cid, c := range mr.mu.clusterRaftGroup {
		c.ClusterInfo.Online = c.Online
		if _, ok := onlineNodes[cid]; !ok {
			c.ClusterInfo.Status = false
			c.ClusterInfo.UpdateCache()
		}
	}
}

func getClusterNodeOK(nCluster uint64) bool {
	return order.G_NodeSates.OK(nCluster)
}

type ModelType int

const (
	M_NORMAL   ModelType = 0
	M_OBSERVER ModelType = 1
	M_WITNESS  ModelType = 2
)

func (mt ModelType) String() string {
	if mt == M_NORMAL {
		return "normal"
	} else if mt == M_OBSERVER {
		return "observer"
	} else if mt == M_WITNESS {
		return "witness"
	}
	return ""
}

func converRoleToStartModel(role string) ModelType {
	switch role {
	case "observer":
		return M_OBSERVER
	case "wintess":
		return M_WITNESS
	default:
		return M_NORMAL
	}
}

func getRole(isObserver, isWitness bool) string {
	var role string
	if isObserver {
		role = "observer"
	} else if isWitness {
		role = "witness"
	} else {
		role = "normal"
	}
	return role
}
