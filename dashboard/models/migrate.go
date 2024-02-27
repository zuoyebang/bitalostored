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

const (
	MigratePrepred int = 0
	MigrateRunning int = 1
	MigrateFinshed int = 2
)

type Migrate struct {
	SID           int            `json:"sid"`
	SourceGroupID int            `json:"source_group_id"`
	TargetGroupID int            `json:"target_group_id"`
	Status        *MigrateStatus `json:"status"`
	CreateTime    string         `json:"create_time"`
	UpdateTime    string         `json:"update_time"`
}

type MigrateStatus struct {
	Unixtime    int64  `json:"unixtime"`
	Costs       int64  `json:"costs"`
	From        string `json:"from"`
	To          string `json:"to"`
	SlotId      int64  `json:"slot_id"`
	Total       int64  `json:"total"`
	Fails       int64  `json:"fails"`
	SuccPercent string `json:"succ_percent"`
	Status      int    `json:"status"`
}

func (g *Migrate) Encode() []byte {
	return jsonEncode(g)
}

func (ms *MigrateStatus) Encode() []byte {
	return jsonEncode(ms)
}
