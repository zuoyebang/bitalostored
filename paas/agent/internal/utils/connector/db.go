package connector

import (
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/config"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var MysqlClientPaas *gorm.DB
var BsClient *gorm.DB

func InitMysql() {
	initPaasClient()
	if !config.C.DisableLogCollect {
		initBsClient()
	}
}

func initPaasClient() {
	var err error
	var user, password, address, database string

	user = config.C.Mysql.Username
	password = config.C.Mysql.Password
	address = config.C.Mysql.Server + ":" + config.C.Mysql.Port
	database = config.C.Mysql.Database

	if config.C.DbMode == "sqlite" {
		MysqlClientPaas, err = gorm.Open(sqlite.Open(config.C.SqlitePath), &gorm.Config{})
		if err != nil {
			logs.Errorf("init mysql error. Error:%+v", err)
		}
	} else {
		ns := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8", user, password, address, database)
		MysqlClientPaas, err = gorm.Open(mysql.Open(ns), &gorm.Config{})
		if err != nil {
			logs.Errorf("init mysql error. Error:%+v", err)
		}
	}
}

func initBsClient() {
	var err error
	var user, password, address, database string
	user = config.C.BsClient.Username
	password = config.C.BsClient.Password
	address = config.C.BsClient.Server + ":" + config.C.BsClient.Port
	database = config.C.BsClient.Database

	ns := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8", user, password, address, database)
	BsClient, err = gorm.Open(mysql.Open(ns), &gorm.Config{})
	if err != nil {
		logs.Errorf("init bs client error. Error:%+v", err)
	}
}

func GetDB(table string) (*gorm.DB, error) {
	db := MysqlClientPaas.Table(table)
	return db, MysqlClientPaas.Error
}
