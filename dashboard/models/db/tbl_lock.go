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

type TblLock struct {
	ID         uint   `gorm:"column:id"`
	LockName   string `gorm:"column:lock_name"`
	CreateTime int64  `gorm:"column:create_time"`
}

var tableLock = "tblLock"

func getLockDB() (*gorm.DB, error) {
	db := global.Table(tableLock)
	return db, global.Error
}

func AddLock(lockName string) error {
	db, err := getLockDB()
	if err != nil {
		return err
	}

	res := &TblLock{
		LockName:   lockName,
		CreateTime: time.Now().Unix(),
	}
	db = db.Create(res)
	return db.Error
}

func DeleteLock(lockName string) error {
	db, err := getLockDB()
	if err != nil {
		return err
	}
	return db.Where("lock_name = ?", lockName).Delete(&TblLock{}).Error
}
