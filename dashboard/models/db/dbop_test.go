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
	"fmt"
	"github.com/zuoyebang/bitalostored/dashboard/internal/consts"
	"testing"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestDBClient(t *testing.T) {
	setDB()
	data, err := getList("/stored/admin")
	if err != nil {
		t.Fail()
		return
	}
	fmt.Println(data)
}

type DashCore struct {
	Token     string `json:"token"`
	StartTime string `json:"start_time"`
	AdminAddr string `json:"admin_addr"`
	HostPort  string `json:"hostport"`

	BackupAddr     string `json:"backup_addr"`
	BackupHostPort string `json:"backup_hostport"`

	ProductName string `json:"product_name"`

	ReadCrossCloud bool `json:"read_cross_cloud"`

	Pid int    `json:"pid"`
	Pwd string `json:"pwd"`
	Sys string `json:"sys"`
}

func TestTransaction(t *testing.T) {
	setDB()
	data, err := readTransaction("/stored/bitalos1/topom-backup")
	if err != nil {
		t.Fail()
		return
	}
	d := &DashCore{}
	if data == nil {
		fmt.Println("data is nil")
		return
	}
	if err := JsonDecode(d, data); err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(data))
}

func listGroup() (interface{}, error) {
	paths, err := getList("/stored/femysql/group")
	if err != nil {
		return nil, err
	}
	group := make(map[int]*Group)
	for _, path := range paths {
		b, err := read(path)
		if err != nil {
			return nil, err
		}
		g := &Group{}
		if err := JsonDecode(g, b); err != nil {
			return nil, err
		}
		group[g.Id] = g
	}
	return group, nil
}

func curd(t *testing.T) {
	err := create("/stored/matrix-live/group/group-1.jj", []byte(`{"haha":"1","age":1}`))
	if err != nil {
		t.Fail()
		return
	}
	data, err := read("/stored/matrix-live/group/group-1.jj")
	if err != nil {
		t.Fail()
		return
	}
	fmt.Println("ashaha", string(data), "hahahaha")
	err = update("/stored/matrix-live/group/group-1.jj", []byte(`{"haha":"guagua","age":2}`))
	if err != nil {
		t.Fail()
	}
	data, err = read("/stored/matrix-live/group/group-1.jj")
	if err != nil {
		t.Fail()
	}
	fmt.Println(string(data))
	err = deleteData("/stored/matrix-live/group/")
	if err != nil {
		t.Fail()
	}
}

func TestSlotActionList(t *testing.T) {
	setDB()
	fmt.Println(SlotActionGetList("lu6"))
}

func TestAddSlotAction(t *testing.T) {
	setDB()
	for i := 0; i < 20; i++ {
		AddSlotAction("lu6", consts.SlotActionTransfer, 1, 2, 1, 2)
		AddSlotAction("lu6", consts.SlotActionRemove, 3, 4, 100, 200)
	}
}

func setDB() {
	dsn := "test:test@tcp(127.0.0.1:8306)/paas_share?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	initDB(db)
}
