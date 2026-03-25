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

package tbl_region

import (
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"time"
)

const TableName = "tblRegion"

type Region struct {
	ID         uint   `gorm:"column:id" json:"regionId"`
	NewId      uint   `gorm:"column:new_id" json:"newRegionId"`
	Name       string `gorm:"column:name" json:"regionName"`
	CreateTime int64  `gorm:"column:create_time" json:"-"`
	UpdateTime int64  `gorm:"column:update_time" json:"-"`
	CreateDate string `gorm:"-" json:"createTime"`
	UpdateDate string `gorm:"-" json:"updateTime"`
}

func GetRegionList(limit int, offset int) ([]*Region, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	if limit > 0 {
		db = db.Limit(limit).Offset(offset)
	}

	res := []*Region{}
	db.Find(&res)
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return res, db.Error
}

func GetList(limit int, offset int) ([]*Region, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	if limit > 0 {
		db = db.Limit(limit).Offset(offset)
	}

	res := []*Region{}
	db.Find(&res)
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return res, db.Error
}

func GetInfo(id uint) (*Region, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("id = ?", id)

	res := &Region{}
	db.First(res)
	return res, db.Error
}

func GetNewRegion(regionId uint) (*Region, error) {
	r, err := GetInfo(regionId)
	if err != nil {
		return nil, err
	}
	if r.NewId == 0 {
		return r, nil
	}
	newInfo, err := GetInfo(r.NewId)
	if err != nil {
		return nil, err
	}
	return newInfo, nil
}

func Create(name string) (*Region, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	res := &Region{Name: name, CreateTime: time.Now().Unix(), UpdateTime: time.Now().Unix()}
	db.Create(res)
	return res, db.Error
}

func Delete(regionId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	return db.Delete(&Region{}, regionId).Error
}

func Update(regionId uint, region *Region) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}

	region.UpdateTime = time.Now().Unix()
	db.First(&Region{}, regionId).UpdateColumns(region)
	return db.Error
}
