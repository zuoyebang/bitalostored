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

type TblCluster struct {
	ID           int            `gorm:"column:id;primary_key" json:"id"`
	Name         string         `gorm:"column:name" json:"name"`
	Status       string         `gorm:"column:status" json:"status"`
	RegionID     int            `gorm:"column:region_id" json:"region_id"`
	StoredID     int            `gorm:"column:stored_id" json:"stored_id"`
	ServiceID    int            `gorm:"column:service_id" json:"service_id"`
	CreateTime   int            `gorm:"column:create_time" json:"create_time"`
	UpdateTime   int            `gorm:"column:update_time" json:"update_time"`
	Monitor      sql.NullString `gorm:"column:monitor" json:"monitor"`
	ConfigPackID int            `gorm:"column:config_pack_id" json:"config_pack_id"`
	Auth         string         `gorm:"column:auth" json:"auth"`
	DeraftToken  string         `gorm:"column:deraft_token" json:"deraft_token"`
	Department   sql.NullString `gorm:"column:department" json:"department"`
	IsStored1    sql.NullInt64  `gorm:"column:is_stored1" json:"is_stored1"`
}

// TableName sets the insert table name for this struct type
func (t *TblCluster) TableName() string {
	return "tblCluster"
}
