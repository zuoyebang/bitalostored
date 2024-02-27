// Copyright 2019 The Bitalostored author and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/zuoyebang/bitalostored/dashboard/dashcore"

	"github.com/docopt/docopt-go"
	"gorm.io/driver/sqlite"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/zuoyebang/bitalostored/dashboard/internal/log"
	"github.com/zuoyebang/bitalostored/dashboard/internal/utils"
	"github.com/zuoyebang/bitalostored/dashboard/models"
)

func main() {
	const usage = `
Usage:
	bitalos-dashboard [--ncpu=N] [--config=CONF] [--log=FILE] [--log-level=LEVEL] [--pidfile=FILE] [--database=ADDR|--sqlite=FILE] [--product_name=NAME] [--product_auth=AUTH]
	bitalos-dashboard  --default-config
	bitalos-dashboard  --version

Options:
	--ncpu=N                    set runtime.GOMAXPROCS to N, default is runtime.NumCPU().
	-c CONF, --config=CONF      run with the specific configuration.
	-l FILE, --log=FILE         set path/name of daliy rotated log file.
	--log-level=LEVEL           set the log-level, should be INFO,WARN,DEBUG or ERROR, default is INFO.
`

	d, err := docopt.ParseArgs(usage, nil, "")
	if err != nil {
		log.PanicError(err, "parse arguments failed")
	}

	switch {

	case d["--default-config"]:
		fmt.Println(dashcore.DefaultConfig)
		return

	case d["--version"].(bool):
		fmt.Println("version:", utils.Version)
		fmt.Println("compile:", utils.Compile)
		return

	}

	if s, ok := utils.Argument(d, "--log"); ok {
		w, err := log.NewRollingFile(s, log.HourlyRolling)
		if err != nil {
			log.PanicErrorf(err, "open log file %s failed", s)
		} else {
			log.StdLog = log.New(w, "")
		}
	}
	log.SetLevel(log.LevelInfo)

	if s, ok := utils.Argument(d, "--log-level"); ok {
		if !log.SetLevelString(s) {
			log.Panicf("option --log-level = %s", s)
		}
	}

	if n, ok := utils.ArgumentInteger(d, "--ncpu"); ok {
		runtime.GOMAXPROCS(n)
	} else {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}
	log.Warnf("set ncpu = %d", runtime.GOMAXPROCS(0))

	config := dashcore.NewDefaultConfig()
	if s, ok := utils.Argument(d, "--config"); ok {
		if err := config.LoadFromFile(s); err != nil {
			log.PanicErrorf(err, "load config %s failed", s)
		}
	}

	var db *gorm.DB

	switch {
	case d["--database"] != nil:
		config.CoordinatorName = "database"
		config.CoordinatorAddr = utils.ArgumentMust(d, "--database")
		log.Warnf("option --database = %s", config.CoordinatorAddr)
		dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
			config.Database.Username, config.Database.Password, config.Database.HostPort, config.Database.DBName)
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
		if err != nil {
			log.PanicErrorf(err, "connect db failed.%+v", err)
		}
	case d["--sqlite"] != nil:
		config.CoordinatorName = "sqlite"
		config.CoordinatorAddr = utils.ArgumentMust(d, "--sqlite")
		db, err = gorm.Open(sqlite.Open(config.CoordinatorAddr), &gorm.Config{})
		if err != nil {
			log.PanicErrorf(err, "connect sqlite failed.%+v", err)
		}
		log.Warnf("option --sqlite = %s", config.CoordinatorAddr)
	default:
		log.Panicf("invalid coordinator")
	}

	if s, ok := utils.Argument(d, "--product_name"); ok {
		config.ProductName = s
		log.Warnf("option --product_name = %s", s)
	}
	if s, ok := utils.Argument(d, "--product_auth"); ok {
		config.ProductAuth = s
		log.Warnf("option --product_auth = %s", s)
	}

	client, err := models.NewClient(config.CoordinatorName, db)
	if err != nil {
		log.PanicErrorf(err, "create '%s' client to '%s' failed", config.CoordinatorName, config.CoordinatorAddr)
	}
	defer client.Close()

	s, err := dashcore.New(client, config)
	if err != nil {
		log.PanicErrorf(err, "create dashcore with config file failed\n%s", config)
	}
	defer s.Close()

	log.Warnf("create dashcore with config\n%s", config)

	if s, ok := utils.Argument(d, "--pidfile"); ok {
		if pidfile, err := filepath.Abs(s); err != nil {
			log.WarnErrorf(err, "parse pidfile = '%s' failed", s)
		} else if err := os.WriteFile(pidfile, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
			log.WarnErrorf(err, "write pidfile = '%s' failed", pidfile)
		} else {
			defer func() {
				if err := os.Remove(pidfile); err != nil {
					log.WarnErrorf(err, "remove pidfile = '%s' failed", pidfile)
				}
			}()
			log.Warnf("option --pidfile = %s", pidfile)
		}
	}

	go func() {
		defer s.Close()
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)

		sig := <-c
		log.Warnf("[%p] dashboard receive signal = '%v'", s, sig)
	}()

	for i := 0; !s.IsClosed() && !s.IsOnline(); i++ {
		if err := s.Start(true); err != nil {
			if i <= 15 {
				log.Warnf("[%p] dashboard online failed [%d]", s, i)
			} else {
				log.Panicf("dashboard online failed, give up & abort :'(")
			}
			time.Sleep(time.Second * 2)
		}

	}

	log.Warnf("[%p] dashboard is working ...", s)

	for !s.IsClosed() {
		time.Sleep(time.Second)
	}

	log.Warnf("[%p] dashboard is exiting ...", s)
}
