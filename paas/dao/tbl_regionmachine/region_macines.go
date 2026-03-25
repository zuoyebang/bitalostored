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

package tbl_regionmachine

import (
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"time"
)

const TableName = "tblRegionMachine"

type RegionMachine struct {
	RegionId   uint   `gorm:"column:region_id" json:"regionId"`
	MachineId  uint   `gorm:"column:machine_id" json:"machineId"`
	CreateTime int64  `gorm:"column:create_time" json:"-"`
	UpdateTime int64  `gorm:"column:update_time" json:"-"`
	CreateDate string `gorm:"-" json:"createTime"`
	UpdateDate string `gorm:"-" json:"updateTime"`
}

func Create(machineIds, regionIds []uint) int {
	if len(machineIds) > 1 && len(regionIds) > 1 {
		log.Error("doesn't support to bind multi region machine")
		return 0
	}
	createFailedNum := 0
	for _, machineId := range machineIds {
		for _, regionId := range regionIds {
			db, err := dao.GetDB(TableName)
			if err != nil {
				log.Errorf("get db failed.err:%+v", err)
				return 0
			}
			res := &RegionMachine{RegionId: regionId, MachineId: machineId, CreateTime: time.Now().Unix(), UpdateTime: time.Now().Unix()}
			db.Create(res)
			if db.Error != nil {
				log.Errorf("db create failed.err:%+v,machineId:%d,regionId:%d", err, machineId, regionId)
				return createFailedNum
			}
			createFailedNum++
		}
	}
	return createFailedNum
}

func DeleteByMachine(machineId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("machine_id = ?", machineId).Delete(&RegionMachine{})

	return db.Error
}

func MultiDeleteMachines(mids []uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("machine_id in (?)", mids).Delete(&RegionMachine{})
	return db.Error
}

func DeleteByRegion(regionId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("region_id = ?", regionId).Delete(&RegionMachine{})

	return db.Error
}

func DeleteRegionMachines(regionId uint, machineIds []uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("region_id = ? and machine_id in (?)", regionId, machineIds).Delete(&RegionMachine{})

	return db.Error
}

func Delete(machineIds, regionIds []uint) int {
	if len(machineIds) > 1 && len(regionIds) > 1 {
		log.Error("doesn't support to bind multi region machine")
		return 0
	}
	successNum := 0
	for _, machineId := range machineIds {
		for _, regionId := range regionIds {
			db, err := dao.GetDB(TableName)
			if err != nil {
				log.Errorf("get db failed.err:%+v", err)
				return 0
			}
			db = db.Where("region_id = ? and machine_id = ?", regionId, machineId).Delete(&RegionMachine{})
			if db.Error != nil {
				log.Errorf("db create failed.err:%+v,machineId:%d,regionId:%d", err, machineId, regionId)
				return successNum
			}
			successNum++
		}
	}
	return successNum
}

func GetMachinesByRegion(regionId uint) ([]uint, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var regionMachines []*RegionMachine
	var machineIds []uint
	//db.Model(&RegionMachine{}).Pluck("machine_id", &machineIds)
	db.Where("region_id = ?", regionId).Find(&regionMachines)
	for _, r := range regionMachines {
		machineIds = append(machineIds, r.MachineId)
	}
	return machineIds, db.Error
}

func GetMachineRegion(machineIds []uint) map[uint][]uint {
	var regionList map[uint][]uint
	regionList = make(map[uint][]uint, 0)
	for _, machineId := range machineIds {
		db, err := dao.GetDB(TableName)
		if err != nil {
			return regionList
		}
		var res []*RegionMachine
		db.Where("machine_id = ?", machineId).Find(&res)
		var regions []uint
		for _, r := range res {
			regions = append(regions, r.RegionId)
		}
		regionList[machineId] = regions
	}
	return regionList
}

func GetRegionMachineCount(regionId uint) int64 {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return 0
	}
	var count int64
	db.Where("region_id = ?", regionId).Count(&count)
	return count
}
