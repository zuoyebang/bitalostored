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

package tbl_cosfile

import (
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"sync"
	"time"
)

var TableName = "tblCosFile"
var createLock sync.Mutex

type CosFile struct {
	ID        uint   `gorm:"column:id;primary_key" json:"id"`
	Name      string `gorm:"column:name" json:"name"`
	CosKey    string `gorm:"column:cos_key" json:"cosKey"`
	FileType  string `gorm:"column:file_type" json:"fileType"`
	FileMode  string `gorm:"column:file_mode" json:"fileMode"`
	Hash      string `gorm:"column:hash" json:"hash"`
	Version   string `gorm:"column:version" json:"version"`
	ServiceId uint   `gorm:"column:service_id" json:"serviceId"`

	CreateTime int64  `gorm:"column:create_time" json:"-"`
	UpdateTime int64  `gorm:"column:update_time" json:"-"`
	CreateDate string `gorm:"-" json:"createTime"`
	UpdateDate string `gorm:"-" json:"updateTime"`
}

func Create(name, cosKey, fileType, fileMode, hash, version string, serviceId uint) (*CosFile, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	createLock.Lock()
	defer createLock.Unlock()

	res := &CosFile{
		Name:       name,
		CosKey:     cosKey,
		FileType:   fileType,
		FileMode:   fileMode,
		Hash:       hash,
		Version:    version,
		ServiceId:  serviceId,
		CreateTime: time.Now().Unix(),
		UpdateTime: time.Now().Unix(),
	}
	db.Create(res)
	return res, db.Error
}

func GetCosFile(id uint) (*CosFile, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	var res CosFile
	db.Where(id).First(&res)
	return &res, db.Error
}

func DeleteFile(id uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db.Delete(&CosFile{}, id)
	return db.Error
}

func GetMaxVersion(serviceId uint) (*CosFile, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var files []*CosFile
	db.Where("service_id = ?", serviceId).Order("update_time desc").Find(&files)
	maxVersion := ""
	var maxVersionFile *CosFile
	for _, f := range files {
		if f.Version > maxVersion {
			maxVersion = f.Version
			maxVersionFile = f
		}
	}
	return maxVersionFile, db.Error
}

func GetListByVersion(serviceId uint, version string) ([]*CosFile, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var files []*CosFile
	db.Where("service_id = ? and version = ?", serviceId, version).Order("update_time desc").Find(&files)
	return files, db.Error
}

func GetList(serviceId uint, clusterId int) ([]*CosFile, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var files []*CosFile
	db.Where("service_id = ? and file_type in ?", serviceId, []string{"main", "lan"}).Order("create_time desc").Find(&files)

	for _, r := range files {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return files, db.Error
}
