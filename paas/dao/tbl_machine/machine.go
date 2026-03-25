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

package tbl_machine

import (
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"time"
)

const TableName = "tblMachine"

type Machine struct {
	ID     uint   `gorm:"column:id;primary_key" json:"machineId"`
	Status string `gorm:"column:status" json:"status"`
	Weight int    `gorm:"column:weight" json:"weight"`
	IDC    string `gorm:"column:idc" json:"idc"`
	IP     string `gorm:"column:ip" json:"ip"`

	NodeSum uint `gorm:"-" json:"-"`

	Budget           string `gorm:"column:budget" json:"budget"`
	HostName         string `gorm:"column:host_name" json:"hostName"`
	NeedUpgrade      string `gorm:"column:need_upgrade" json:"needUpgrade"`
	UpgradeVersionId uint   `gorm:"column:upgrade_version" json:"upgradeVersion"`
	UpgradeConfig    string `gorm:"column:upgrade_config" json:"upgradeConfig"`
	AgentConfig      string `gorm:"column:agent_config" json:"agentConfig"`
	Version          string `gorm:"column:version" json:"version"`
	CpuSet           string `gorm:"column:cpu_set" json:"cpuSet"`
	ShareCpuSet      string `gorm:"column:share_cpu_set" json:"shareCpuSet"`
	CpuTotal         int    `gorm:"column:cpu_total" json:"cpuTotal"`
	CpuSetMax        int    `gorm:"column:cpu_set_max" json:"cpuSetMax"`
	IsVirtual        int    `gorm:"column:is_virtual" json:"isVirtual"`

	CreateTime int64  `gorm:"column:create_time" json:"-"`
	UpdateTime int64  `gorm:"column:update_time" json:"-"`
	CreateDate string `gorm:"-" json:"createTime"`
	UpdateDate string `gorm:"-" json:"updateTime"`
}

func GetOnlineListByIds(machineIds []uint) ([]*Machine, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var machines []*Machine
	db = db.Where("id in (?) and status = ?", machineIds, def.MACHINE_STATUS_ONLINE).Find(&machines)
	for _, r := range machines {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return machines, db.Error
}

func GetList(machineIds []uint) ([]*Machine, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var machines []*Machine
	db = db.Where(machineIds).Find(&machines)
	for _, r := range machines {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	if db.Error != nil {
		log.Errorf("query %s %v failed: [%v]", TableName, machineIds, db.Error)
	}
	return machines, db.Error
}

func GetOnlineList(machineIds []uint, idc string) ([]*Machine, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var machines []*Machine
	db = db.Where("id in (?) and status = ? and idc = ?", machineIds, def.MACHINE_STATUS_ONLINE, idc).Find(&machines)
	for _, r := range machines {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return machines, db.Error
}

func DeleteMachine(machineId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("id = ?", machineId).Delete(&Machine{})

	return db.Error
}

func MultiDeleteMachine(mids []uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("id in (?)", mids).Delete(&Machine{})
	return db.Error
}

func GetMachineInfo(ip string) (*Machine, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var machines []*Machine
	db = db.Where("ip = ?", ip).Find(&machines)
	if len(machines) == 0 || len(machines) > 1 {
		return nil, errors.New("machine IP does not exist")
	}
	return machines[0], db.Error
}

func GetOnlineMachines() ([]*Machine, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var machines []*Machine
	db = db.Where("status = ? and weight > 0", def.MACHINE_STATUS_ONLINE).Find(&machines)
	return machines, db.Error
}

func GetMasterOnlineMachines() ([]*Machine, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	var machines []*Machine
	db = db.Begin()
	db = db.Where("status = ? and weight > 0", def.MACHINE_STATUS_ONLINE).Find(&machines)
	if db.Error != nil {
		db.Rollback()
	}
	db.Commit()
	return machines, db.Error
}

func GetAllMachines() ([]*Machine, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var machines []*Machine
	db = db.Find(&machines)
	return machines, db.Error
}

func GetMachinesByBudget(budget string) ([]*Machine, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var machines []*Machine
	db = db.Where("budget = ?", budget).Find(&machines)
	return machines, db.Error
}

func GetMachinesByIdc(idc string) ([]*Machine, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var machines []*Machine
	db = db.Where("idc = ?", idc).Find(&machines)
	return machines, db.Error
}

func GetMachinesByBudgetIdc(budget string, idc string, isVirtual int) ([]*Machine, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var machines []*Machine
	if isVirtual == 2 {
		db = db.Where("budget = ? and idc = ?", budget, idc).Find(&machines)
	} else {
		db = db.Where("budget = ? and idc = ? and is_virtual = ?", budget, idc, isVirtual).Find(&machines)
	}

	return machines, db.Error
}

func GetMachinesByIpList(ip []string) ([]*Machine, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var machines []*Machine
	db = db.Where("ip in (?) and status = ?", ip, def.MACHINE_STATUS_ONLINE).Find(&machines)
	return machines, db.Error
}

func GetInfo(machineId uint) (*Machine, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var m Machine
	db = db.First(&m, machineId)
	if db.Error != nil {
		log.Errorf("machine getInfo failed,err:%v", db.Error)
	}
	return &m, db.Error
}

func GetInfoUseMaster(machineId uint) (*Machine, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var m Machine
	tx := db.Begin()
	tx = tx.First(&m, machineId)
	tx.Commit()
	return &m, tx.Error
}

func Create(weight int, idc string, ip, budget string) (*Machine, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	res := &Machine{
		Status:     def.MACHINE_STATUS_ONLINE,
		Weight:     weight,
		IDC:        idc,
		IP:         ip,
		Budget:     budget,
		CreateTime: time.Now().Unix(),
		UpdateTime: time.Now().Unix(),
	}
	db = db.Create(res)
	return res, db.Error
}
func Update(machineId uint, m Machine) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	m.UpdateTime = time.Now().Unix()
	db = db.First(&Machine{}, machineId).UpdateColumns(m)
	return db.Error
}

func MultiOfflineMachine(machineIds []uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}

	db = db.Where("id in (?)", machineIds).Updates(Machine{UpdateTime: time.Now().Unix(), Status: def.MACHINE_STATUS_OFFLINE})
	return db.Error
}
