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

const MaxSlotNum = 1024

type Slot struct {
	Id                   int               `json:"id"`
	Locked               bool              `json:"locked"`
	Switched             bool              `json:"switched"`
	MasterAddr           string            `json:"master_addr"`
	MasterAddrGroupId    int               `json:"master_addr_group_id"`
	RoundRobinNum        uint64            `json:"round_robin_num"`
	LocalCloudServers    []string          `json:"local_servers"`
	BackupCloudServers   []string          `json:"backup_servers"`
	WitnessServers       []string          `json:"witness_servers"`
	GroupServersCloudMap map[string]string `json:"group_servers_cloudmap"`
	GroupServersStats    map[string]bool   `json:"group_servers_stats"`
}

func (s *Slot) Encode() []byte {
	return jsonEncode(s)
}

type SlotMapping struct {
	Id      int `json:"id"`
	GroupId int `json:"group_id"`

	Action struct {
		NotMigrateData bool   `json:"not_migrate_data,omitempty"`
		Index          int    `json:"index,omitempty"`
		State          string `json:"state,omitempty"`
		TargetId       int    `json:"target_id,omitempty"`
	} `json:"action"`
}

func (m *SlotMapping) Encode() []byte {
	return jsonEncode(m)
}
