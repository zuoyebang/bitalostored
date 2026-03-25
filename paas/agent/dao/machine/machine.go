package tbl_machine

import (
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/connector"
	"gorm.io/gorm"
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
	NeedUpgrade      string `gorm:"column:need_upgrade" json:"needUpgrade"`
	UpgradeVersionId uint   `gorm:"column:upgrade_version" json:"upgradeVersion"`
	UpgradeConfig    string `gorm:"column:upgrade_config" json:"upgradeConfig"`
	AgentConfig      string `gorm:"column:agent_config" json:"agentConfig"`
	Version          string `gorm:"column:version" json:"version"`

	HostName  string `gorm:"column:host_name" json:"hostName"`
	CpuTotal  int    `gorm:"column:cpu_total" json:"cpuTotal"`
	CpuSetMax int    `gorm:"column:cpu_set_max" json:"cpuSetMax"`

	CpuSet      string `gorm:"column:cpu_set" json:"cpuSet"`
	ShareCpuSet string `gorm:"column:share_cpu_set" json:"shareCpuSet"`

	CreateTime int64  `gorm:"column:create_time" json:"-"`
	UpdateTime int64  `gorm:"column:update_time" json:"-"`
	CreateDate string `gorm:"-" json:"createTime"`
	UpdateDate string `gorm:"-" json:"updateTime"`
}

func GetMachinesByIpList(ip []string) ([]*Machine, error) {
	db, err := connector.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	var machines []*Machine
	db = db.Where("ip in (?)", ip).Find(&machines)
	return machines, db.Error
}

func GetMachinesByIp(ip string) (*Machine, error) {
	db, err := connector.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	machine := new(Machine)
	db = db.Where("ip = ?", ip).First(machine)
	if db.Error == gorm.ErrRecordNotFound {
		return machine, nil
	}
	return machine, db.Error
}

func UpdateHostname(id uint, hostname string) error {
	db, err := connector.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("id = ?", id).Update("host_name", hostname)
	return db.Error
}

func UpdateTime(machineId uint, m Machine) error {
	db, err := connector.GetDB(TableName)
	if err != nil {
		return err
	}
	m.UpdateTime = time.Now().Unix()
	db = db.First(&Machine{}, machineId).UpdateColumns(m)
	return db.Error
}
