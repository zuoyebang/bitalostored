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

package dbclient

import "gorm.io/gorm"

type Machine struct {
	ID     uint   `gorm:"column:id;primary_key" json:"machineId"`
	Status string `gorm:"column:status" json:"status"`
	Weight int    `gorm:"column:weight" json:"weight"`
	IDC    string `gorm:"column:idc" json:"idc"`
	IP     string `gorm:"column:ip" json:"ip"`

	NodeSum uint `gorm:"-" json:"-"`

	Budget           string `gorm:"column:budget" json:"budget"`
	HostName         string `gorm:"column:host_name" json:"hostName"`
	NeedUpgrade      string `gorm:"column:need_upgrade" json:"needUpgrade"`
	UpgradeVersionId uint   `gorm:"column:upgrade_version" json:"upgradeVersion"`
	UpgradeConfig    string `gorm:"column:upgrade_config" json:"upgradeConfig"`
	AgentConfig      string `gorm:"column:agent_config" json:"agentConfig"`
	Version          string `gorm:"column:version" json:"version"`
	CpuSet           string `gorm:"column:cpu_set" json:"cpuSet"`
	ShareCpuSet      string `gorm:"column:share_cpu_set" json:"shareCpuSet"`
	CpuTotal         int    `gorm:"column:cpu_total" json:"cpuTotal"`
	CpuSetMax        int    `gorm:"column:cpu_set_max" json:"cpuSetMax"`

	CreateTime int64  `gorm:"column:create_time" json:"-"`
	UpdateTime int64  `gorm:"column:update_time" json:"-"`
	CreateDate string `gorm:"-" json:"createTime"`
	UpdateDate string `gorm:"-" json:"updateTime"`
}

var tableMachine = "tblMachine"

func getMachineDB() (*gorm.DB, error) {
	db := global.Table(tableMachine)
	return db, global.Error
}

type MachineIdc struct {
	ID  uint   `gorm:"column:id;primary_key" json:"machineId"`
	IDC string `gorm:"column:idc" json:"idc"`
}

func GetMachineIdcs() ([]*MachineIdc, error) {
	db, err := getMachineDB()
	if err != nil {
		return nil, err
	}
	var res []*MachineIdc
	db = db.Select("id, idc").Where("status = ?", "online")
	db = db.Find(&res)
	return res, db.Error
}
