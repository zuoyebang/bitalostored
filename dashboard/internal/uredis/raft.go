// Copyright 2019 The Bitalostored author and other contributors.
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

package uredis

import (
	"errors"
	"fmt"
	"time"

	"github.com/zuoyebang/bitalostored/dashboard/internal/log"

	jsoniter "github.com/json-iterator/go"
)

type MembershipV2 struct {
	Info Membership `json:"info"`
}

type Membership struct {
	// ConfigChangeID is the Raft entry index of the last applied membership
	// change entry.
	ConfigChangeID uint64 `json:"config_changeid"`
	// Nodes is a map of NodeID values to NodeHost Raft addresses for all regular
	// Raft nodes.
	Nodes map[uint64]string `json:"nodes"`
	// Observers is a map of NodeID values to NodeHost Raft addresses for all
	// observers in the Raft cluster.
	Observers map[uint64]string `json:"observers"`
	// Witnesses is a map of NodeID values to NodeHost Raft addrsses for all
	// witnesses in the Raft cluster.
	Witnesses map[uint64]string `json:"witnesses"`
	// Removed is a set of NodeID values that have been removed from the Raft
	// cluster. They are not allowed to be added back to the cluster.
	Removed map[uint64]struct{} `json:"removed"`
}

func (ms *MembershipV2) Marshal() (string, error) {
	data, err := jsoniter.Marshal(ms)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (ms *MembershipV2) CheckNodeIsUse(nodeId int) bool {
	uintNodeId := uint64(nodeId)
	if _, ok := ms.Info.Nodes[uintNodeId]; ok {
		return true
	}
	if _, ok := ms.Info.Observers[uintNodeId]; ok {
		return true
	}
	if _, ok := ms.Info.Witnesses[uintNodeId]; ok {
		return true
	}
	return false
}

func (ms *MembershipV2) CheckNodeIsRemove(nodeId int) bool {
	uintNodeId := uint64(nodeId)
	_, ok := ms.Info.Removed[uintNodeId]
	return ok
}

func (s *InfoCache) GetNodeRaftInfo(addr string, iswitness bool) (*NodeInfo, error) {
	if nf, exist := s.loadNodeInfo(addr); exist {
		if nf.isDown {
			return nil, errors.New("node is down err")
		}
		return nf, nil
	}

	nodeInfo := &NodeInfo{}

	addrInfo := s.Get(addr, true)
	if len(addrInfo["status"]) <= 0 {
		time.Sleep(30 * time.Millisecond)
		addrInfo = s.Get(addr, true)
	}

	if len(addrInfo["status"]) <= 0 {
		if iswitness {
			log.Warnf("GetNodeRaftInfo witness not alive [isdown:true] [addr:%s] [status:%v] [data:%v]", addr, nodeInfo.NodeStatus, addrInfo)
		} else {
			log.Warnf("GetNodeRaftInfo not alive [isdown:true] [addr:%s] [status:%v] [data:%v]", addr, nodeInfo.NodeStatus, addrInfo)
		}
		nodeInfo.isDown = true
		s.storeNodeInfo(addr, nodeInfo)
		return nil, errors.New("node is down err")
	}

	nodeInfo.Role = addrInfo["role"]
	nodeInfo.CurrentNodeId = addrInfo["current_node_id"]
	nodeInfo.CurrentAddress = addr
	nodeInfo.StartModel = addrInfo["start_model"]
	nodeInfo.ClusterId = addrInfo["cluster_id"]
	nodeInfo.LeaderNodeId = addrInfo["leader_node_id"]
	nodeInfo.ClusterNodes = addrInfo["cluster_nodes"]

	if addrInfo["status"] == "true" {
		nodeInfo.NodeStatus = true
	} else {
		nodeInfo.NodeStatus = false
		if iswitness {
			log.Warnf("GetNodeRaftInfo witness not alive [isdown:false] [addr:%s] [status:%s] [data:%v]", addr, nodeInfo.NodeStatus, addrInfo)
		} else {
			log.Warnf("GetNodeRaftInfo not alive [isdown:false] [addr:%s] [status:%s] [data:%v]", addr, nodeInfo.NodeStatus, addrInfo)
		}
	}

	s.storeNodeInfo(addr, nodeInfo)
	return nodeInfo, nil
}

type RaftGroupStatusInfo struct {
	NodeInfoList map[string]*NodeInfo
	FlagNodeList map[string]bool
	MasterAddr   string
}

func (s *InfoCache) GetRaftGroupStatusInfo(groupId int, addrs map[string]string) *RaftGroupStatusInfo {
	pingNodeList := make(map[string]bool)
	var leaderAddress, deraftAddr string
	var hasDeraft bool

	for addr := range addrs {
		nodeInfo, err := s.GetNodeRaftInfo(addr, false)
		if err == nil {
			if nodeInfo.NodeStatus {
				if nodeInfo.StartModel == "normal" {
					pingNodeList[addr] = true
					if nodeInfo.Role == "master" {
						leaderAddress = addr
					} else if nodeInfo.Role == "single" && !hasDeraft {
						hasDeraft = true
						deraftAddr = addr
					}
				}
			} else {
				if nodeInfo.StartModel == "normal" {
					pingNodeList[addr] = true
				}
			}
		} else {
			pingNodeList[addr] = false
		}
	}

	if hasDeraft && len(pingNodeList) == 1 {
		leaderAddress = deraftAddr
	}

	return &RaftGroupStatusInfo{
		MasterAddr: leaderAddress,
	}
}

func (s *InfoCache) GetRaftGroupMaster(groupId int, addrs map[string]string) (string, error) {
	rgsi := s.GetRaftGroupStatusInfo(groupId, addrs)

	if len(rgsi.MasterAddr) > 0 {
		return rgsi.MasterAddr, nil
	}
	return "", errors.New(fmt.Sprintf("group-[%d] raft status error", groupId))
}
