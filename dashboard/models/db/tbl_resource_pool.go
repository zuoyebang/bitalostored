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

import (
	"gorm.io/gorm"
)

type Resource struct {
	ID           uint   `gorm:"column:id" json:"id"`
	ClusterName  string `gorm:"column:cluster_name" json:"clusterName"`
	ClusterId    uint   `gorm:"column:cluster_id" json:"clusterId"`
	ServiceId    uint   `gorm:"column:service_id" json:"serviceId"`
	Port         uint   `gorm:"column:port" json:"port"`
	CpuSetType   int    `gorm:"column:cpu_set_type" json:"cpuSetType"`
	IDC          string `gorm:"column:idc" json:"idc"`
	MetricName   string `gorm:"column:metric_name" json:"metricName"`
	CgroupLimit  int16  `gorm:"column:cgroup_limit" json:"cgroupLimit"`
	SuggestValue int64  `gorm:"column:suggest_value" json:"suggestValue"`
	ManualValue  int64  `gorm:"column:manual_value" json:"manualValue"`
	SyncTime     int64  `gorm:"column:sync_time" json:"-"`
	SyncDate     string `gorm:"-" json:"syncTime"`
	ApplyTime    int64  `gorm:"column:apply_time" json:"-"`
	ApplyDate    string `gorm:"-" json:"applyTime"`
	CreateTime   int64  `gorm:"column:create_time" json:"-"`
	CreateDate   string `gorm:"-" json:"createTime"`
	UpdateTime   int64  `gorm:"column:update_time" json:"-"`
	UpdateDate   string `gorm:"-" json:"updateTime"`
}

var tableResource = "tblResourcePool"

func getResourceDB() (*gorm.DB, error) {
	db := global.Table(tableResource)
	return db, global.Error
}

func GetCgroupByClusterName(clusterName string) ([]*Resource, error) {
	db, err := getResourceDB()
	if err != nil {
		return nil, err
	}

	var res []*Resource
	db = db.Where("cluster_name = ?", clusterName)
	db = db.Find(&res)
	return res, db.Error
}

func GetCgroup() ([]*Resource, error) {
	db, err := getResourceDB()
	if err != nil {
		return nil, err
	}

	var res []*Resource
	db = db.Find(&res)
	return res, db.Error
}
