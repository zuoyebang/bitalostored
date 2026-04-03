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

package dao

import (
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/log"

	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var global *gorm.DB

func InitDB(conf config.Database) (err error) {
	var db *gorm.DB
	if conf.Database == "sqlite" {
		if len(conf.FilePath) <= 0 {
			return fmt.Errorf("sqlite file path is empty")
		}
		db, err = gorm.Open(sqlite.Open(conf.FilePath), &gorm.Config{})
		if err != nil {
			return err
		}
	} else {
		ns := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8", conf.Username, conf.Password, conf.Hostport, conf.Database)
		db, err = gorm.Open(mysql.Open(ns), &gorm.Config{
			Logger: logger.New(NewGormLogger(*log.GetLogger()), logger.Config{LogLevel: logger.Info}),
		})
		if err != nil {
			return err
		}
	}
	log.Infof("open db: %s", conf.Database)
	global = db
	return
}

func GetDB(table string) (*gorm.DB, error) {
	db := global.Table(table)
	return db, global.Error
}

type GormLogger struct {
	logger log.Logger
}

func NewGormLogger(logger log.Logger) *GormLogger {
	return &GormLogger{logger: logger}
}

func (l *GormLogger) Printf(msg string, data ...interface{}) {
	l.logger.Infof(msg, data...)
}
