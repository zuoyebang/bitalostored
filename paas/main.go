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

package main

import (
	"fmt"
	"github.com/spf13/pflag"
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_operation"
	"github.com/zuoyebang/bitalostored/paas/model"
	"github.com/zuoyebang/bitalostored/paas/router"
	"github.com/zuoyebang/bitalostored/paas/utils"
	"github.com/zuoyebang/bitalostored/paas/utils/collector"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/middleware"
	"strconv"

	"github.com/gin-gonic/gin"
)

type operationRecorder struct{}

func (r *operationRecorder) Create(url, uid, opData string, operationTime int64) error {
	return tbl_operation.Create(url, uid, opData, operationTime)
}

func init() {
	logPath := pflag.String("log", "", "set path/name of daliy rotated log file.")
	configPath := pflag.String("conf", "", "run with the specific configuration.")
	pflag.Parse()
	if *logPath != "" {
		log.NewLogger(&log.Options{
			IsDebug:      false,
			RotationTime: "Daily",
			LogPath:      *logPath,
		})
	}
	log.Infof("log path -> %s", *logPath)

	var err error
	if *configPath != "" {
		err = config.SetConf(*configPath)
		if err != nil {
			log.Errorf("read config file failed.err:%+v", err)
			return
		}
	}
	log.Infof("config path -> %s", *configPath)

	if err = dao.InitDB(config.GetConf().Database); err != nil {
		log.Errorf("open database failed.err:%+v", err)
		return
	}
	if err = model.Init(); err != nil {
		log.Fatal(err, "init model data failed")
	}
}
func main() {
	appEngine := gin.New()

	if config.GetConf().PaasServer.PaaSEnv == def.ServerEnvOnline && len(config.GetConf().PaasServer.GrafanaUrl) > 0 {
		collector.NewGrafanaCollector()
	}

	recorder := &operationRecorder{}
	q := utils.NewQueue(100, recorder)
	appEngine.Use(middleware.Recovery())
	appEngine.Use(func(context *gin.Context) {
		context.Set(utils.QueueOperation, q)
		context.Next()
	})
	if err := router.SetRouter(appEngine).Run(fmt.Sprintf(":%s", strconv.Itoa(config.GetConf().PaasServer.ListenPort))); err != nil {
		log.Errorf("start stored paas failed, err:%v", err)
	}
}
