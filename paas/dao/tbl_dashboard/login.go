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

package tbl_dashboard

import (
	"encoding/json"
	"errors"
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
)

const TableName = "tblDashboard"

type Dashboard struct {
	ID          uint   `gorm:"column:id;primary_key" json:"userId"`
	ProductName string `gorm:"column:product_name" json:"-"`
	SubPath     string `gorm:"column:sub_path" json:"-"`
	FullPath    string `gorm:"column:full_path" json:"-"`

	Value      string     `gorm:"column:value;type:text" json:"-"`
	UserDetail UserDetail `gorm:"-" json:"value"`

	CreateTime int64 `gorm:"column:create_time" json:"-"`
	UpdateTime int64 `gorm:"column:update_time" json:"-"`
}

type UserDetail struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Role     uint   `json:"role"`
	Auth     string `json:"auth"`
}

func GetUserAccount(username string) (*UserDetail, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		log.Warn("failed to connect tblDashboard.err:", err)
		return nil, err
	}
	db = db.Where("product_name = ? and sub_path = ? ", "paasadmin", username)
	res := &Dashboard{}
	db.First(res)
	if db.Error != nil {
		return nil, db.Error
	}
	if res.ID <= 0 {
		return nil, nil
	}
	err = json.Unmarshal([]byte(res.Value), &res.UserDetail)
	if err != nil {
		return nil, err
	}
	return &res.UserDetail, nil
}

func VerifyUser(username, password string) bool {
	db, err := dao.GetDB(TableName)
	if err != nil {
		log.Warn("failed to connect tblDashboard.err:", err)
		return false
	}
	db = db.Where("product_name = ? and sub_path = ? ", "paasadmin", username)
	res := &Dashboard{}
	db.First(res)
	if db.Error != nil || res.ID < 1 {
		return false
	}
	err = json.Unmarshal([]byte(res.Value), &res.UserDetail)
	if err != nil {
		return false
	}
	if len(res.UserDetail.Auth) > 0 {
		pwd := math2.GetMd5(password + res.UserDetail.Auth)
		if res.UserDetail.Username != username || res.UserDetail.Password != pwd {
			return false
		}
		return true
	}

	if res.UserDetail.Username != username || res.UserDetail.Password != password {
		return false
	}
	return true
}

func DeleteCluster(name string) error {
	if name == "" {
		return errors.New("cluster is empty")
	}
	db, err := dao.GetDB(TableName)
	if err != nil {
		log.Warn("failed to connect tblDashboard.err:", err)
		return err
	}
	db = db.Where("product_name = ?", name).Delete(&Dashboard{})
	return db.Error
}
