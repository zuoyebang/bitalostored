// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tbl_group

import (
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"gorm.io/gorm"
	"sync"
	"time"
)

const TableName = "tblGroup"

type Group struct {
	ClusterId  uint   `gorm:"column:cluster_id" json:"clusterId"`
	GroupId    uint   `gorm:"column:group_id" json:"groupId"`
	Status     string `gorm:"column:status" json:"groupStatus"`
	InitRaft   string `gorm:"column:init_raft" json:"initRaft"`
	InitNodeId string `gorm:"column:init_node_id" json:"initNodeId"`
	MaxNodeId  uint   `gorm:"column:max_node_id" json:"maxNodeId"`
	ServiceId  uint   `gorm:"column:service_id" json:"serviceId"`
	Lock       bool   `gorm:"column:locked" json:"lock"`

	CreateTime int64  `gorm:"column:create_time" json:"-"`
	UpdateTime int64  `gorm:"column:update_time" json:"-"`
	CreateDate string `gorm:"-" json:"createTime"`
	UpdateDate string `gorm:"-" json:"updateTime"`
}

func GetList(clusterId uint, status string, limit int, offset int) ([]*Group, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	if status != "" {
		db = db.Where("cluster_id = ? and status in (?)", clusterId, []string{status})
	} else {
		db = db.Where("cluster_id = ?", clusterId)
	}
	if limit >= 0 {
		db = db.Limit(limit).Offset(offset)
	}

	res := []*Group{}
	db.Find(&res)
	if db.Error != nil {
		log.Errorf("query %s %d failed: [%v]", TableName, clusterId, db.Error)
	}
	return res, db.Error
}

func LockGroup(clusterId, groupId uint, lock bool) bool {
	uniqueIDLock.Lock()
	defer uniqueIDLock.Unlock()
	db, err := dao.GetDB(TableName)
	if err != nil {
		return false
	}
	res := &Group{}
	db = db.Where("cluster_id = ? and group_id = ?", clusterId, groupId).First(res)
	if db.Error != nil {
		return false
	}
	if res.Lock == lock {
		return false
	}
	mGroup := make(map[string]interface{}, 0)
	mGroup["cluster_id"] = clusterId
	mGroup["group_id"] = groupId
	mGroup["locked"] = lock
	db = db.Where("cluster_id = ? and group_id = ?", clusterId, groupId).Updates(mGroup)
	if db.Error != nil {
		return false
	}
	return true
}

func GetGroupLock(clusterId, groupId uint) bool {
	uniqueIDLock.Lock()
	defer uniqueIDLock.Unlock()
	db, err := dao.GetDB(TableName)
	if err != nil {
		return false
	}
	res := &Group{}
	db = db.Where("cluster_id = ? and group_id = ?", clusterId, groupId).First(res)
	if db.Error != nil {
		return false
	}
	return res.Lock
}

func DeleteByCluster(clusterId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("cluster_id = ?", clusterId).Delete(&Group{})

	return db.Error
}

func Delete(clusterId, groupId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("cluster_id = ? and group_id = ?", clusterId, groupId).Delete(&Group{})

	return db.Error
}

func GetInfo(clusterId, groupId uint) (*Group, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("cluster_id = ? and group_id = ?", clusterId, groupId)

	res := &Group{}
	db.First(res)
	return res, db.Error
}
func Create(clusterId, serviceId uint) (*Group, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	uniqueIDLock.Lock()
	defer uniqueIDLock.Unlock()
	count, err := GetClusterGroupCount(clusterId)
	if err != nil {
		return nil, err
	}
	res := &Group{
		ClusterId:  clusterId,
		Status:     def.GROUP_STATUS_ONLINE,
		ServiceId:  serviceId,
		GroupId:    uint(count) + 1,
		CreateTime: time.Now().Unix(),
		UpdateTime: time.Now().Unix(),
	}
	db.Create(res)
	return res, db.Error
}

func Update(clusterId, groupId uint, group Group) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}

	group.UpdateTime = time.Now().Unix()
	db = db.Where("cluster_id = ? and group_id = ?", clusterId, groupId).UpdateColumns(group)
	if db.Error != nil {
		log.Errorf("update group failed-clusterId:%d-groupId:%d-err:%v", clusterId, groupId, db.Error)
	}
	return db.Error
}

func UpdateSql(clusterId, groupId uint, group Group) (string, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return "", err
	}
	group.UpdateTime = time.Now().Unix()
	stmt := db.Session(&gorm.Session{DryRun: true}).Where("cluster_id = ? and group_id = ?", clusterId, groupId).UpdateColumns(group).Statement
	finalSQL := db.Dialector.Explain(stmt.SQL.String(), stmt.Vars...)
	return finalSQL, nil
}

func GetClusterGroupCount(clusterId uint) (int, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return 0, err
	}
	db = db.Where("cluster_id = ?", clusterId)
	res := []*Group{}
	db.Find(&res)
	max := 0
	for _, g := range res {
		if int(g.GroupId) > max {
			max = int(g.GroupId)
		}
	}
	return max, err
}

var uniqueIDLock sync.Mutex

func GetServerGroup() ([]*Group, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("service_id = 6")
	res := make([]*Group, 0)
	db.Find(&res)
	return res, db.Error
}
