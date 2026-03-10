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
	jsoniter "github.com/json-iterator/go"
	"github.com/zuoyebang/bitalosraft"
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

type NodeHostInfoV2 struct {
	Info bitalosraft.NodeHostInfo `json:"info"`
}

type Membership struct {
	ConfigChangeID uint64              `json:"config_changeid"`
	Nodes          map[uint64]string   `json:"nodes"`
	Observers      map[uint64]string   `json:"observers"`
	NonVotings     map[uint64]string   `json:"nonvotings"`
	Witnesses      map[uint64]string   `json:"witnesses"`
	Removed        map[uint64]struct{} `json:"removed"`
}

type MembershipV2 struct {
	Info Membership `json:"info"`
}

func (m *MembershipV2) InitByDragonboatMembership(ms *bitalosraft.Membership) {
	m.Info.ConfigChangeID = ms.ConfigChangeID
	m.Info.Nodes = ms.Nodes
	m.Info.Observers = ms.NonVotings
	m.Info.NonVotings = ms.NonVotings
	m.Info.Witnesses = ms.Witnesses
	m.Info.Removed = ms.Removed
}

func (m *MembershipV2) Marshal() (string, error) {
	data, err := jsoniter.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
