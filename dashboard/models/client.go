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

package models

import (
	"time"

	"github.com/zuoyebang/bitalostored/dashboard/internal/errors"
	mysqlclient "github.com/zuoyebang/bitalostored/dashboard/models/db"

	"gorm.io/gorm"
)

type Client interface {
	Create(path string, data []byte) error
	Update(path string, data []byte) error
	Delete(path string) error

	Read(path string) ([]byte, error)
	List(path string) ([]string, error)
	Details(path string) ([]string, error)
	SubList(subPath string) (interface{}, error)

	Close() error
}

func NewClient(coordinator string, db *gorm.DB) (Client, error) {
	switch coordinator {
	case "db", "database":
		return mysqlclient.New(db)
	case "sqlite":
		SqliteInit(db)
		return mysqlclient.New(db)
	}
	return nil, errors.Errorf("invalid coordinator name = %s", coordinator)
}

func SqliteInit(db *gorm.DB) {
	_ = db.AutoMigrate(&mysqlclient.TblDashboard{})
	var res []*mysqlclient.TblDashboard
	db = db.Where("product_name = ?", "admin").Find(&res)
	if len(res) <= 0 {
		dt := &mysqlclient.TblDashboard{
			ClusterName: "admin",
			SubPath:     "demo",
			FullPath:    "/stored/admin/demo",
			Value:       "{\"username\":\"demo\",\"password\":\"demo\",\"role\":1}",
			CreateTime:  time.Now().Unix(),
			UpdateTime:  time.Now().Unix(),
		}
		db.Create(dt)
	}
}
