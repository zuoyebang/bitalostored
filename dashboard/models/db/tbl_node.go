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

type Node struct {
	ClusterId uint   `gorm:"column:cluster_id" json:"clusterId"`
	GroupId   uint   `gorm:"column:group_id" json:"groupId"`
	NodeId    uint   `gorm:"column:node_id" json:"nodeId"`
	Status    string `gorm:"column:status" json:"nodeStatus"`

	RegionId  uint `gorm:"column:region_id" json:"regionId"`
	MachineId uint `gorm:"column:machine_id" json:"machineId"`
	ServiceId uint `gorm:"column:service_id" json:"serviceId"`

	IsWitness      bool   `gorm:"column:is_witness" json:"isWitness"`
	CosFileId      uint   `gorm:"column:cos_file_id" json:"cosFileId"`
	CosFileVersion string `gorm:"column:cos_file_version" json:"version"`
	ConfigContent  string `gorm:"column:config_content" json:"configContent"`

	ServicePort uint `gorm:"column:service_port" json:"servicePort"`
	ClusterPort uint `gorm:"column:cluster_port" json:"clusterPort"`

	CreateTime int64  `gorm:"column:create_time" json:"-"`
	UpdateTime int64  `gorm:"column:update_time" json:"-"`
	CreateDate string `gorm:"-" json:"createTime"`
	UpdateDate string `gorm:"-" json:"updateTime"`
}

var tableNode = "tblNode"

func getNodeDB() (*gorm.DB, error) {
	db := global.Table(tableNode)
	return db, global.Error
}

type OnlineNode struct {
	MachineId   uint `gorm:"column:machine_id" json:"machineId"`
	GroupId     uint `gorm:"column:group_id" json:"groupId"`
	ServicePort uint `gorm:"column:service_port" json:"servicePort"`
	ClusterId   uint `gorm:"column:cluster_id" json:"clusterId"`
	IsWitness   bool `gorm:"column:is_witness" json:"isWitness"`
	ServiceId   uint `gorm:"column:service_id" json:"serviceId"`
}

func GetOnlineNode() ([]*OnlineNode, error) {
	db, err := getNodeDB()
	if err != nil {
		return nil, err
	}
	var res []*OnlineNode
	db = db.Select("machine_id,group_id,service_port,cluster_id,is_witness,service_id").Where("status = ?", "online")
	db = db.Find(&res)
	return res, db.Error
}
