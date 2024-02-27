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

package models

const MaxGroupId = 9999

type Group struct {
	Id          int            `json:"id"`
	Servers     []*GroupServer `json:"servers"`
	MasterAddr  string         `json:"master_addr,omitempty"`
	OutOfSync   bool           `json:"out_of_sync,omitempty"`
	IsExpanding bool           `json:"is_expanding"`

	Promoting struct {
		Index int    `json:"index,omitempty"`
		State string `json:"state,omitempty"`
	} `json:"promoting"`
}

type GroupServer struct {
	Addr         string `json:"server"`
	DataCenter   string `json:"datacenter"`
	CloudType    string `json:"cloudtype"`
	ServerRole   string `json:"server_role"`
	ReplicaGroup bool   `json:"replica_group"`

	Action struct {
		Index int    `json:"index,omitempty"`
		State string `json:"state,omitempty"`
	} `json:"action"`
}

const (
	ServerMasterSlaveNode = "master_slave_node"
	ServerOberserNode     = "observer_node"
	ServerWitnessNode     = "witness_node"
	ServerDeRaftNode      = "deraft_single_node"
)

func CheckInServerRole(role string) bool {
	if role == ServerMasterSlaveNode || role == ServerOberserNode || role == ServerWitnessNode || role == ServerDeRaftNode {
		return true
	}
	return false
}

func (g *Group) Encode() []byte {
	return jsonEncode(g)
}
