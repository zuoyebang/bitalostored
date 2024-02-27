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

package raft

import (
	braft "github.com/zuoyebang/bitalostored/raft"

	jsoniter "github.com/json-iterator/go"
)

type RetType int

const (
	R_UNKNOWN_ERROR RetType = -1
	R_SUCCESS       RetType = 0
	R_SHUT_DOWN     RetType = 1
	R_NIL_POINTER   RetType = 2
	R_ERROR         RetType = 3
	R_PARA_ERR      RetType = 4
	R_RETRY_EXHAUST RetType = 5
	R_REJECTED      RetType = 6
	R_ABORTED       RetType = 7
	R_RET_ERROR     RetType = 8
	R_JOSON_ERR     RetType = 9
)

type NodeInfo struct {
	ClusterID uint64 `json:"clusterID"`
	NodeID    uint64 `json:"nodeID"`
}

type ClusterInfo struct {
	ClusterID         uint64            `json:"clusterID"`
	NodeID            uint64            `json:"nodeID"`
	Nodes             map[uint64]string `json:"nodes"`
	ConfigChangeIndex uint64            `json:"configChangeIndex"`
	StateMachineType  uint64            `json:"stateMachineType"`
	IsLeader          bool              `json:"isLeader"`
	IsObserver        bool              `json:"isObserver"`
	IsWitness         bool              `json:"isWitness "`
	Pending           bool              `json:"pending"`
}

type NodeHostInfo struct {
	RaftAddress     string        `json:"raftAddress"`
	ClusterInfoList []ClusterInfo `json:"clusterInfoList"`
	LogInfo         []NodeInfo    `json:"LogInfo"`
}

type NodeHostInfoV2 struct {
	Info braft.NodeHostInfo `json:"info"`
}

type MembershipV2 struct {
	Info Membership `json:"info"`
}

func (m *MembershipV2) InitByDragonboatMembership(ms *braft.Membership) {
	m.Info.ConfigChangeID = ms.ConfigChangeID
	m.Info.Nodes = ms.Nodes
	m.Info.Observers = ms.NonVotings
	m.Info.NonVotings = ms.NonVotings
	m.Info.Witnesses = ms.Witnesses
	m.Info.Removed = ms.Removed
}

type Membership struct {
	ConfigChangeID uint64              `json:"config_changeid"`
	Nodes          map[uint64]string   `json:"nodes"`
	Observers      map[uint64]string   `json:"observers"`
	NonVotings     map[uint64]string   `json:"nonvotings"`
	Witnesses      map[uint64]string   `json:"witnesses"`
	Removed        map[uint64]struct{} `json:"removed"`
}

func (m *MembershipV2) Marshal() (string, error) {
	data, err := jsoniter.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
