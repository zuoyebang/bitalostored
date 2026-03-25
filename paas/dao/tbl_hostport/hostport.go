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

package tbl_hostport

import (
	"github.com/zuoyebang/bitalostored/paas/dao"
	"time"
)

const TableName = "tblHostPort"

type MachinePort struct {
	MachineId uint   `gorm:"column:machine_id" json:"machineId"`
	ID        uint   `gorm:"column:id;primary_key" json:"id"`
	Port      uint   `gorm:"column:port" json:"port"`
	IP        string `gorm:"column:ip" json:"ip"`

	CreateTime int64 `gorm:"column:create_time" json:"createTime"`
	UpdateTime int64 `gorm:"column:update_time" json:"updateTime"`
}

func DeleteByMachine(machineId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}

	db = db.Where("machine_id = ?", machineId).Delete(&MachinePort{})
	return db.Error
}

func MultiDeleteByMachine(mids []uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("machine_id in (?)", mids).Delete(&MachinePort{})
	return db.Error
}

func DeleteById(id uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}

	db = db.Where("id = ?", id).Delete(&MachinePort{})
	return db.Error
}

func GetList(machineId uint, limit int, offset int) ([]*MachinePort, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	db = db.Where("machine_id = ?", machineId)
	if limit > 0 {
		db = db.Limit(limit).Offset(offset)
	}

	var list []*MachinePort
	db.Find(&list)
	return list, db.Error
}

func GetMachinePorts(machineId uint, portMin, portMax int) ([]*MachinePort, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	if portMin >= portMax {
		return nil, nil
	}

	db = db.Where("machine_id = ? and port > ? and port < ?", machineId, portMin, portMax)

	var list []*MachinePort
	db.Find(&list)
	return list, db.Error
}

func GetMaxMachinePorts(machineId uint, portMin, portMax int) (uint, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return 0, err
	}
	if portMin >= portMax {
		return 0, nil
	}

	var maxPort uint
	db = db.Select("max(port)").Where("machine_id = ? and port > ? and port < ?", machineId, portMin, portMax)
	db.Find(&maxPort)
	return maxPort, db.Error
}

func GetMaxMachinePort(machineId uint) (uint, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return 0, err
	}

	var maxPort uint
	db = db.Select("max(port)").Where("machine_id = ?", machineId)
	db.Find(&maxPort)
	return maxPort, db.Error
}

func GetIdlePortList(portMin, portMax, fetchNum int) ([]uint, []uint, error) {
	if portMin >= portMax {
		return nil, nil, nil
	}
	existPorts := make([]uint, 0, 100)
	idlePorts := make([]uint, 0, 100)
	var res []*MachinePort
	for port := portMin; port <= portMax; port++ {
		db, err := dao.GetDB(TableName)
		if err != nil {
			return nil, nil, err
		}
		db = db.Select("port").Where("port = ?", port).Limit(1)
		db.Find(&res)
		if len(res) >= 1 {
			existPorts = append(existPorts, uint(port))
		} else {
			idlePorts = append(idlePorts, uint(port))
		}
		if len(idlePorts) >= fetchNum {
			break
		}
	}
	return existPorts, idlePorts, nil
}

type Mpr struct {
	MachineID uint `gorm:"column:machine_id"`
	MaxPort   uint `gorm:"column:mp"`
}

func GetMaxPortByMids(machineId []uint) (map[uint]uint, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	var ret []Mpr
	//db = db.Select("machine_id, max(port) as mp").Where("machine_id in ? and port >= ? and port <= ?", machineId, portMin, portMax).Group("machine_id")
	db = db.Select("machine_id, max(port) as mp").Where("machine_id in ? ", machineId).Group("machine_id")
	db.Find(&ret)
	res := make(map[uint]uint)
	for _, r := range ret {
		res[r.MachineID] = r.MaxPort
	}
	return res, db.Error
}

func IsExist(id, port uint) bool {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return false
	}
	var res []*MachinePort
	db = db.Where("machine_id = ? and port = ?", id, port).Find(&res)
	if db.Error != nil || len(res) == 0 {
		return false
	}
	return true
}

func Update(id uint, machinePort MachinePort) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}

	machinePort.UpdateTime = time.Now().Unix()
	db.First(&MachinePort{}, id).UpdateColumns(machinePort)
	return db.Error
}

func Create(machineId uint, port uint, ip string) (*MachinePort, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	res := &MachinePort{
		MachineId:  machineId,
		Port:       port,
		IP:         ip,
		CreateTime: time.Now().Unix(),
		UpdateTime: time.Now().Unix(),
	}
	db = db.Create(res)
	return res, db.Error
}

type SortPort []*MachinePort

func (l SortPort) Len() int {
	return len(l)
}

func (l SortPort) Less(i, j int) bool {
	return l[i].Port < l[j].Port
}
func (l SortPort) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
