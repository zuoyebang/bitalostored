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

type Cluster struct {
	Id     uint   `gorm:"column:id;primary_key" json:"clusterId"`
	Name   string `gorm:"column:name" json:"clusterName"`
	Status string `gorm:"column:status" json:"clusterStatus"`

	RegionId  uint `gorm:"column:region_id" json:"regionId"`
	ServiceId uint `gorm:"column:service_id" json:"serviceId"`
	StoredId  uint `gorm:"column:stored_id" json:"storedId"`

	ConfigPackId uint   `gorm:"column:config_pack_id" json:"configPackId"`
	StoredAuth   string `gorm:"column:auth" json:"storedAuth"`
	DeraftToken  string `gorm:"column:deraft_token" json:"deraftToken"`
	Department   string `gorm:"column:department" json:"department"`
	IsStored1    bool   `gorm:"column:is_stored1" json:"isStored1"`
	ClusterGroup string `gorm:"column:cluster_group" json:"clusterGroup"`

	MonitorLink string `gorm:"column:monitor" json:"monitorLink"`

	CreateTime int64  `gorm:"column:create_time" json:"-"`
	UpdateTime int64  `gorm:"column:update_time" json:"-"`
	CreateDate string `gorm:"-" json:"createTime"`
	UpdateDate string `gorm:"-" json:"updateTime"`
}

var tableCluster = "tblCluster"

func getClusterDB() (*gorm.DB, error) {
	db := global.Table(tableCluster)
	return db, global.Error
}

type ProxyServers struct {
	Id         uint   `gorm:"column:id;primary_key" json:"clusterId"`
	Department string `gorm:"column:department" json:"department"`
	ServiceId  uint   `gorm:"column:service_id" json:"serviceId"`
	Name       string `gorm:"column:name" json:"clusterName"`
}

func GetProxyServer() ([]*ProxyServers, error) {
	db, err := getClusterDB()
	if err != nil {
		return nil, err
	}
	var res []*ProxyServers
	db = db.Select("id, department,service_id,name").Where("status = ? and service_id in (1,2,6)", "online").Order("name")
	db = db.Find(&res)
	return res, db.Error
}

func GetClusterByClusterName(name string, serviceId uint) (*Cluster, error) {
	db, err := getClusterDB()
	if err != nil {
		return nil, err
	}
	var res *Cluster
	db = db.Select("monitor").Where("service_id = ? and name = ? and status = ?", serviceId, name, "online")
	db = db.Find(&res)
	return res, db.Error
}
