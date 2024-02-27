// Copyright 2019 The Bitalostored author and other contributors.
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
	"errors"
	"fmt"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"gorm.io/gorm"
)

type TblDashboard struct {
	ID          uint   `gorm:"column:id"`
	ClusterName string `gorm:"column:product_name"`
	SubPath     string `gorm:"column:sub_path"`
	FullPath    string `gorm:"column:full_path"`
	Value       string `gorm:"column:value"`
	CreateTime  int64  `gorm:"column:create_time"`
	UpdateTime  int64  `gorm:"column:update_time"`
}

type Group struct {
	Id         int           `json:"id"`
	Servers    []interface{} `json:"servers"`
	MasterAddr string        `json:"master_addr,omitempty"`

	Promoting struct {
		Index int    `json:"index,omitempty"`
		State string `json:"state,omitempty"`
	} `json:"promoting"`

	OutOfSync   bool  `json:"out_of_sync,omitempty"`
	IsExpanding bool  `json:"is_expanding"`
	UpdateTime  int64 `json:"update_time"`
}

func JsonDecode(v interface{}, b []byte) error {
	if err := jsoniter.Unmarshal(b, v); err != nil {
		return err
	}
	return nil
}

var global *gorm.DB

const table string = "tblDashboard"

func (TblDashboard) TableName() string {
	return table
}

func initDB(db *gorm.DB) {
	global = db
}
func getDB() (*gorm.DB, error) {
	db := global.Table(table)
	return db, global.Error
}

func getList(path string) ([]string, error) {
	db, err := getDB()
	if err != nil {
		return nil, err
	}
	var res []string
	productName, subPath := extractPath(path)
	if productName == "" && subPath == "" {
		db = db.Model(&TblDashboard{}).Distinct().Pluck("product_name", &res)
	} else if productName != "" && subPath == "" {
		db = db.Where("product_name = ?", productName).Distinct().Pluck("sub_path", &res)
	} else if productName != "" && subPath != "" {
		db = db.Where("product_name = ? and sub_path = ?", productName, subPath).Distinct().Pluck("full_path", &res)
	}

	return res, db.Error
}

func getSubList(subPath string) (interface{}, error) {
	db, err := getDB()
	if err != nil {
		return nil, err
	}
	var res []*TblDashboard
	db = db.Where("sub_path = ?", subPath).Find(&res)
	return res, db.Error
}

func deleteData(path string) error {
	db, err := getDB()
	if err != nil {
		return err
	}
	productName, subPath := extractPath(path)
	if productName == "" {
		return nil
	}
	if subPath == "" {
		return db.Where("product_name = ?", productName).Delete(&TblDashboard{}).Error
	}
	pathEles := strings.Split(strings.TrimLeft(path, "/"), "/")
	if len(pathEles) >= 4 && pathEles[3] != "" {
		return db.Where("full_path = ?", path).Delete(&TblDashboard{}).Error
	}
	return db.Where("product_name = ? and sub_path = ?", productName, subPath).Delete(&TblDashboard{}).Error
}

func getDetails(path string) ([]string, error) {
	db, err := getDB()
	if err != nil {
		return nil, nil
	}
	data := []TblDashboard{}
	db = db.Where("product_name = ? and sub_path = 'group' ", path).Find(&data)

	if errors.Is(db.Error, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	var res []string
	for i := 0; i < len(data); i++ {
		res = append(res, data[i].Value)
	}
	return res, db.Error
}

func read(path string) ([]byte, error) {
	db, err := getDB()
	if err != nil {
		return []byte{}, nil
	}
	res := &TblDashboard{}
	db = db.Where("full_path = ?", path).First(res)
	if errors.Is(db.Error, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return []byte(res.Value), db.Error
}

func create(path string, data []byte) error {
	if isExists(path) {
		return errors.New(fmt.Sprintf("record exists.full_path:%s", path))
	}
	db, err := getDB()
	if err != nil {
		return err
	}
	clusterName, subPath := extractPath(path)
	res := &TblDashboard{
		ClusterName: clusterName,
		SubPath:     subPath,
		FullPath:    path,
		Value:       string(data),
		CreateTime:  time.Now().Unix(),
		UpdateTime:  time.Now().Unix(),
	}
	return db.Create(res).Error
}

func update(path string, data []byte) error {
	db, err := getDB()
	if err != nil {
		return err
	}
	if !isExists(path) {
		return create(path, data)
	}
	return db.Where("full_path = ?", path).UpdateColumns(TblDashboard{Value: string(data), UpdateTime: time.Now().Unix()}).Error
}

func isExists(path string) bool {
	db, _ := getDB()
	res := &TblDashboard{}
	db.Where("full_path = ?", path).First(res)
	if errors.Is(db.Error, gorm.ErrRecordNotFound) {
		return false
	}
	if res.ID > 0 {
		return true
	}
	return false
}

func extractPath(path string) (string, string) {
	pathEles := strings.Split(strings.TrimLeft(path, "/"), "/")
	if len(pathEles) == 2 {
		return pathEles[1], ""
	}
	if len(pathEles) >= 3 {
		return pathEles[1], pathEles[2]
	}
	return "", ""
}
