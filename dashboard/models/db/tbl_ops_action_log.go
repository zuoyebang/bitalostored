package dbclient

import (
	"gorm.io/gorm"
	"time"
)

const TableName = "tblOpsActionLog"

const ServerChangeMaster = 110
const ServerReplica = 111

type OpsActionLog struct {
	ID          uint   `gorm:"column:id" json:"id"`
	Ip          string `gorm:"column:ip" json:"ip"`
	Port        uint   `gorm:"column:port" json:"port"`
	ClusterName string `gorm:"column:cluster_name" json:"clusterName"`
	ActionType  int    `gorm:"column:action_type" json:"actionType"`
	OpName      string `gorm:"column:op_name" json:"opName"`
	UpdateTime  int64  `gorm:"column:update_time" json:"-"`
	CreateTime  int64  `gorm:"column:create_time" json:"createTime"`
}

func getOpsActionLogDB() (*gorm.DB, error) {
	db := global.Table(TableName)
	return db, global.Error
}

func CreateOpsActionLog(ip string, port uint, clusterName string, actionType int) error {
	db, err := getOpsActionLogDB()
	if err != nil {
		return err
	}
	opTime := time.Now().Unix()
	actionLog := &OpsActionLog{
		Ip:          ip,
		Port:        port,
		ClusterName: clusterName,
		ActionType:  actionType,
		OpName:      "",
		UpdateTime:  opTime,
		CreateTime:  opTime,
	}
	db = db.Create(actionLog)
	return db.Error
}
