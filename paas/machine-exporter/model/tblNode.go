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

type TblNode struct {
	NodeID         int            `gorm:"column:node_id;primary_key" json:"node_id"`
	Status         string         `gorm:"column:status" json:"status"`
	ClusterID      int            `gorm:"column:cluster_id" json:"cluster_id"`
	GroupID        int            `gorm:"column:group_id" json:"group_id"`
	RegionID       int            `gorm:"column:region_id" json:"region_id"`
	MachineID      int            `gorm:"column:machine_id" json:"machine_id"`
	ServiceID      int            `gorm:"column:service_id" json:"service_id"`
	PackageID      int            `gorm:"column:package_id" json:"package_id"`
	ServicePort    int            `gorm:"column:service_port" json:"service_port"`
	ClusterPort    int            `gorm:"column:cluster_port" json:"cluster_port"`
	CreateTime     int            `gorm:"column:create_time" json:"create_time"`
	UpdateTime     int            `gorm:"column:update_time" json:"update_time"`
	CosFileID      int            `gorm:"column:cos_file_id" json:"cos_file_id"`
	CpuThrottledNr int            `gorm:"column:cpu_throttled_nr" json:"cpu_throttled_nr"`
	CosFileVersion string         `gorm:"column:cos_file_version" json:"cos_file_version"`
	ConfigContent  sql.NullString `gorm:"column:config_content" json:"config_content"`
	IsWitness      sql.NullInt64  `gorm:"column:is_witness" json:"is_witness"`
	TblCluster     TblCluster     `gorm:"foreignKey:id;references:cluster_id"`
}

// TableName sets the insert table name for this struct type
func (t *TblNode) TableName() string {
	return "tblNode"
}
