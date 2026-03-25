package main

import (
	"github.com/zuoyebang/bitalostored/paas/agent/agent"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/config"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/flock"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"github.com/zuoyebang/bitalostored/paas/agent/router"
	"github.com/zuoyebang/bitalostored/paas/agent/service/collector"
	"os"
	"path/filepath"

	"github.com/docopt/docopt-go"
	"github.com/gin-gonic/gin"
)

func init() {
	const usage = `
Usage:
	stored-monitor [--log=FILE] [--config=CONF] [--recovery=START]

Options:
    -l FILE, --log=FILE          set path/name of no rotated running log file .
	-c CONF, --config=CONF       run with the specific configuration.
	-r START, --recovery=START   recovery machine node.
`

	d, err := docopt.ParseArgs(usage, nil, "0.1.0")
	if err != nil {
		logs.Warn("parse argv error:", err)
		return
	}
	if s, ok := utils.Argument(d, "--log"); ok {
		logs.NewLogger(&logs.Options{
			IsDebug:      false,
			RotationTime: "Daily",
			LogPath:      s,
		})
	}

	configPath, err := d.String("--config")
	if err != nil {
		logs.Error(err)
		return
	}
	logs.Infof("config path -> %s", configPath)
	err = config.SetConfig(configPath)
	if err != nil {
		logs.Errorf("read config file failed.err:%+v", err)
		return
	}
	if s, ok := utils.Argument(d, "--recovery"); ok {
		if s == "start" {
			agent.SetRecovery()
		}
	}
}

func main() {
	agent.RecoveryNode()
	lockFile := filepath.Join(config.C.AgentPath, "Agent.LOCK")
	lock, e := flock.Create(lockFile)
	if e != nil {
		logs.Warn("create lock file failed.err:", e)
		os.Exit(2)
	}
	defer lock.Release()

	e = lock.Lock()
	if e != nil {
		logs.Warn("lock file failed.err:", e)
		os.Exit(2)
	}
	flock.IsLocked = true
	defer func() {
		if flock.IsLocked {
			lock.Unlock()
			flock.IsLocked = false
		}
	}()
	go agent.Start(lock)
	if !config.C.DisableLogCollect {
		go collector.Start(config.C.Area)
	}
	engine := gin.New()
	httpServer(engine)
}

func httpServer(engine *gin.Engine) {
	router.Http(engine)
	port := config.GetWebPort()

	if err := engine.Run(port); err != nil {
		panic(err.Error())
	}
}
