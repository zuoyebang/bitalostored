package helper

import (
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"time"
)

var MysqlPass *gorm.DB

func InitMysql() {
	var (
		err error
	)
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?&parseTime=True", GetConf().Database.Username, GetConf().Database.Password,
		GetConf().Database.Hostport, GetConf().Database.Database)
	MysqlPass, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	sqlDB, err := MysqlPass.DB()
	if err != nil {
		return
	}
	sqlDB.SetMaxIdleConns(10)

	sqlDB.SetMaxOpenConns(1000)

	sqlDB.SetConnMaxLifetime(3600 * time.Second)
}
