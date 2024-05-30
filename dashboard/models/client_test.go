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
	"fmt"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var client Client

func init() {
	var err error
	coordinator := "sqlite"
	if client, err = NewClient(coordinator, nil); err != nil {
		fmt.Println("init Client err:", err.Error())
	}
}

func TestCreateTable(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("dh.db"), &gorm.Config{})
	if err != nil {
		t.Errorf("open sqlite err:%v", err)
	}
	sql := "CREATE TABLE `tblDashboard` (\n  `id` INTEGER PRIMARY KEY AUTOINCREMENT,\n  `product_name` varchar(512) NOT NULL DEFAULT '',\n  `sub_path` varchar(512) NOT NULL DEFAULT '',\n  `full_path` varchar(512) NOT NULL DEFAULT '',\n  `value` text,\n  `create_time` int unsigned NOT NULL DEFAULT '0',\n  `update_time` int unsigned NOT NULL DEFAULT '0'\n);"
	db = db.Exec(sql)
	t.Logf("db err=%v", db.Error)
}

func TestInsert(t *testing.T) {
	err := client.Create("s", []byte("sss"))
	if err != nil {
		t.Errorf("create error:%v", err)
		return
	}
	r, err := client.Read("s")
	if err != nil {
		t.Errorf("list error:%v", err)
		return
	}
	t.Logf("list %v", r)
}

func TestRead(t *testing.T) {
	r, err := client.Read("s")
	if err != nil {
		t.Errorf("read error:%v", err)
		return
	}
	t.Logf("read %s", string(r))
}
