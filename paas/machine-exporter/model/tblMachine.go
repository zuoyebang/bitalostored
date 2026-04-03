package model

import (
	"database/sql"
	"time"

	"github.com/guregu/null"
)

var (
	_ = time.Second
	_ = sql.LevelDefault
	_ = null.Bool{}
)

type TblMachine struct {
	ID             int            `gorm:"column:id;primary_key" json:"id"`
	Status         string         `gorm:"column:status" json:"status"`
	Weight         int            `gorm:"column:weight" json:"weight"`
	Idc            string         `gorm:"column:idc" json:"idc"`
	IP             string         `gorm:"column:ip" json:"ip"`
	CreateTime     int            `gorm:"column:create_time" json:"create_time"`
	UpdateTime     int            `gorm:"column:update_time" json:"update_time"`
	Budget         string         `gorm:"column:budget" json:"budget"`
	NeedUpgrade    string         `gorm:"column:need_upgrade" json:"need_upgrade"`
	UpgradeVersion int            `gorm:"column:upgrade_version" json:"upgrade_version"`
	UpgradeConfig  sql.NullString `gorm:"column:upgrade_config" json:"upgrade_config"`
	AgentConfig    sql.NullString `gorm:"column:agent_config" json:"agent_config"`
	Version        string         `gorm:"column:version" json:"version"`
	TblNode        []TblNode      `gorm:"foreignKey:machine_id;references:id"`
}

// TableName sets the insert table name for this struct type
func (t *TblMachine) TableName() string {
	return "tblMachine"
}
