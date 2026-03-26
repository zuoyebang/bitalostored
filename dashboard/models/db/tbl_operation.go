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

type OperationRecord struct {
	ID         uint   `gorm:"column:id" json:"regionId"`
	URL        string `gorm:"column:url" json:"url"`
	Module     string `gorm:"column:module" json:"module"`
	Uid        string `gorm:"column:uid" json:"uid"`
	Cookie     string `gorm:"column:cookie" json:"cookie"`
	CreateTime int64  `gorm:"column:create_time" json:"-"`
	UpdateTime int64  `gorm:"column:update_time" json:"-"`
}

func getOPDB() (*gorm.DB, error) {
	db := global.Table("tblOperationRecord")
	return db, global.Error
}

func AddOperationRecord(url, uid, cookie string) error {
	db, err := getOPDB()
	if err != nil {
		return err
	}

	res := &OperationRecord{
		URL:        url,
		Module:     "dashboard",
		Uid:        uid,
		Cookie:     cookie,
		CreateTime: time.Now().Unix(),
		UpdateTime: time.Now().Unix(),
	}
	db = db.Create(res)
	return db.Error
}
