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

package tbl_operation

import (
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"time"
)

const TableName = "tblOperationRecord"

type OperationRecord struct {
	ID         uint   `gorm:"column:id" json:"id"`
	URL        string `gorm:"column:url" json:"url"`
	Module     string `gorm:"column:module" json:"module"`
	Uid        string `gorm:"column:uid" json:"uid"`
	OpData     string `gorm:"column:cookie" json:"cookie"`
	CreateTime int64  `gorm:"column:create_time" json:"-"`
	UpdateTime int64  `gorm:"column:update_time" json:"-"`
	CreateDate string `gorm:"-" json:"createTime"`
	UpdateDate string `gorm:"-" json:"updateTime"`
}

func Create(url, uid, opData string, operationTime int64) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}

	var opTime int64
	if operationTime > 0 {
		opTime = operationTime
	} else {
		opTime = time.Now().Unix()
	}
	res := &OperationRecord{
		URL:        url,
		Module:     "stored-paas",
		Uid:        uid,
		OpData:     opData,
		CreateTime: opTime,
		UpdateTime: opTime,
	}
	db = db.Create(res)
	return db.Error
}

func GetList(uid, module string, startTime, endTime int64, page int, num int) ([]*OperationRecord, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	if len(uid) > 0 {
		db = db.Where("uid = ?", uid)
	}
	if len(module) > 0 {
		db = db.Where("module = ?", module)
	}
	if startTime > 0 {
		db = db.Where("create_time >= ?", startTime)
	}
	if endTime > 0 {
		db = db.Where("create_time <= ?", endTime)
	}

	db = db.Order("create_time DESC").Offset((page - 1) * num).Limit(num)

	var res []*OperationRecord
	db.Find(&res)
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return res, db.Error
}

func GetCount(uid string) (int64, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return 0, err
	}
	if len(uid) > 0 {
		db = db.Where("uid = ?", uid)
	}
	var num int64
	db.Count(&num)
	return num, db.Error
}

type OperationUids struct {
	Uid string
}

func GetUidList() ([]*OperationUids, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	var res []*OperationUids
	db = db.Select("distinct(uid)").Find(&res)
	return res, db.Error
}
