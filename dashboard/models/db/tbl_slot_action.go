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
	"time"

	"gorm.io/gorm"
)

type SlotAction struct {
	ID          uint   `gorm:"column:id" json:"id"`
	ClusterName string `gorm:"column:cluster_name" json:"clusterName"`
	Action      string `gorm:"column:action" json:"action"`
	SlotStart   int    `gorm:"column:slot_start" json:"slotStart"`
	SlotEnd     int    `gorm:"column:slot_end" json:"slotEnd"`
	SrcGroup    int    `gorm:"column:src_group" json:"srcGroup"`
	DstGroup    int    `gorm:"column:dst_group" json:"dstGroup"`
	CreateTime  int64  `gorm:"column:create_time" json:"-"`
	CreateDate  string `gorm:"-" json:"createTime"`
	UpdateTime  int64  `gorm:"column:update_time" json:"-"`
	UpdateDate  string `gorm:"-" json:"updateTime"`
}

var tableSlotAction = "tblSlotAction"

func getSlotActionDB() (*gorm.DB, error) {
	db := global.Table(tableSlotAction)
	return db, global.Error
}

func AddSlotAction(clusterName, action string, slotStart, slotEnd int, srcGroup, dstGroup int) error {
	db, err := getSlotActionDB()
	if err != nil {
		return err
	}

	res := &SlotAction{
		ClusterName: clusterName,
		Action:      action,
		SlotStart:   slotStart,
		SlotEnd:     slotEnd,
		SrcGroup:    srcGroup,
		DstGroup:    dstGroup,
		CreateTime:  time.Now().Unix(),
		UpdateTime:  time.Now().Unix(),
	}
	db = db.Create(res)
	return db.Error
}

func SlotActionGetList(clusterName string) ([]*SlotAction, error) {
	db, err := getSlotActionDB()
	if err != nil {
		return nil, err
	}
	var list []*SlotAction
	err = db.Where("cluster_name = ?", clusterName).Order("create_time desc").Find(&list).Error
	if err == nil {
		for _, v := range list {
			v.CreateDate = time.Unix(v.CreateTime, 0).Format("2006-01-02 15:04:05")
			v.UpdateDate = time.Unix(v.CreateTime, 0).Format("2006-01-02 15:04:05")
		}
	}
	return list, err
}
