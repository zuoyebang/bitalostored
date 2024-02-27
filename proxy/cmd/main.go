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
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/zuoyebang/bitalostored/proxy/internal/anticc"
	"github.com/zuoyebang/bitalostored/proxy/internal/cgroup"
	"github.com/zuoyebang/bitalostored/proxy/internal/config"
	"github.com/zuoyebang/bitalostored/proxy/internal/dashboard"
	"github.com/zuoyebang/bitalostored/proxy/internal/log"
	"github.com/zuoyebang/bitalostored/proxy/internal/switcher"
	"github.com/zuoyebang/bitalostored/proxy/proxy"

	"github.com/spf13/pflag"
)

func main() {
	configPath := pflag.String("config", "", "")
	pidFile := pflag.String("pidfile", "", "")
	dashboardName := pflag.String("dashboard", "", "")
	pflag.Parse()

	cfg := config.NewDefaultConfig()
	if *configPath != "" {
		if err := cfg.LoadFromFile(*configPath); err != nil {
			panic(fmt.Sprintf("load config failed err:%s", err.Error()))
		}
	}

	initLogger(cfg)
	initCgroup(cfg)
	initPprof(cfg)

	if err := anticc.LoadConfig(cfg); err != nil {
		log.Fatalf("load security config failed err:%v", err)
	}

	p, err := proxy.New(cfg)
	if err != nil {
		log.Fatalf("create proxy failed err:%s", err.Error())
	}

	log.Infof("create proxy with config\n%s", cfg)

	if *pidFile != "" {
		if pidfile, err := filepath.Abs(*pidFile); err != nil {
			log.Warnf("parse pidfile:%s failed err:%s", pidfile, err.Error())
		} else if err := os.WriteFile(pidfile, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
			log.Warnf("write pidfile:%s failed err:%s", pidfile, err.Error())
		} else {
			defer func() {
				if err := os.Remove(pidfile); err != nil {
					log.Warnf("remove pidfile:%s failed err:%s", pidfile, err.Error())
				}
			}()
			log.Infof("option --pidfile = %s", *pidFile)
		}
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)

	if *dashboardName != "" {
		log.Infof("option --dashboard = %s", *dashboardName)
		go autoOnlineWithDashboard(p, *dashboardName, cfg.ProductName)
	}

	go func() {
		for !p.IsClosed() && !p.IsOnline() {
			log.Warnf("proxy waiting online ...")
			time.Sleep(time.Second)
		}

		log.Info("proxy is working ...")
	}()

	<-sc

	log.Info("proxy is closing ...")
	p.Close()
	log.CloseLog()
	log.Info("proxy is closed ...")
}

func autoOnlineWithDashboard(p *proxy.Proxy, dashboardName, clusterName string) {
	for i := 0; i < 10; i++ {
		if p.IsClosed() || p.IsOnline() {
			return
		}
		if onlineProxy(p, dashboardName, clusterName) {
			return
		}
		time.Sleep(time.Second * 3)
	}
	log.Fatal("online proxy failed")
}

func onlineProxy(p *proxy.Proxy, dashboardName, clusterName string) bool {
	cfg := p.Config()

	client := dashboard.NewApiClient(dashboardName, cfg)
	t, err := client.ModelFE(clusterName)
	if err != nil {
		log.Warnf("rpc fetch modelFE failed clusterName:%s err:%s", clusterName, err.Error())
		t, err = client.Model()
		if err != nil {
			log.Warnf("rpc fetch model failed clusterName:%s err:%s", clusterName, err.Error())
			return false
		}
	}

	if t.ProductName != cfg.ProductName {
		log.Fatalf("unexpected product name, got model\n%s", t.Encode())
	}
	client.SetXAuth(cfg.ProductName)

	if cfg.ReadCrossCloud == config.CrossCloudOverwrite {
		switcher.ReadCrossCloud.Store(t.ReadCrossCloud)
	}

	adminAddr := p.Model().AdminAddr
	if err = client.OnlineProxyFE(adminAddr, clusterName); err != nil {
		log.Warnf("proxy onlinefe register dashboard failed adminAddr:%s clusterName:%s err:%s", adminAddr, clusterName, err.Error())
		if err = client.OnlineProxy(adminAddr); err != nil {
			log.Warnf("proxy online register dashboard failed adminAddr:%s clusterName:%s err:%s", adminAddr, clusterName, err.Error())
			return false
		}
	}

	log.Info("proxy online register dashboard ok")
	return true
}

func initCgroup(cfg *config.Config) {
	runtime.GOMAXPROCS(cfg.MaxProcs)

	if len(cfg.ProxyAddr) <= 0 || len(cfg.ProductName) <= 0 {
		log.Errorf("initCgroup failed empty proxy_addr:%s product_name:%s", cfg.ProxyAddr, cfg.ProductName)
		return
	}

	addrs := strings.Split(cfg.ProxyAddr, ":")
	if len(addrs) != 2 {
		log.Errorf("initCgroup ProxyAddr split failed addr:%s", cfg.ProxyAddr)
		return
	}
	proxyAddr := addrs[1]
	cgroupPath := fmt.Sprintf("/sys/fs/cgroup/cpu/stored/proxy_%s_%s", cfg.ProductName, proxyAddr)
	cpuAdjuster := cgroup.NewCpuAdjust(cgroupPath, cfg.MaxProcs)
	cpuAdjuster.Run()
}

func initLogger(cfg *config.Config) {
	opts := &log.Options{
		IsDebug:       cfg.Log.IsDebug,
		RotationTime:  cfg.Log.RotationTime,
		LogFile:       cfg.Log.LogFile,
		StatsLogFile:  cfg.Log.StatsLogFile,
		SlowLog:       cfg.Log.SlowLog,
		SlowLogFile:   cfg.Log.SlowLogFile,
		AccessLog:     cfg.Log.AccessLog,
		AccessLogFile: cfg.Log.AccessLogFile,
	}
	log.NewLogger(opts)
}

func initPprof(cfg *config.Config) {
	if cfg.PprofSwitch != 1 || len(cfg.PprofAddress) <= 0 {
		return
	}

	go func() {
		pprofAddr := cfg.PprofAddress
		if err := http.ListenAndServe(pprofAddr, nil); err != nil {
			log.Errorf("pprof listen failed err:%s", err.Error())
		}
	}()
}
