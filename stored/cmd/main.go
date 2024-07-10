// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
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
	"syscall"

	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/raft"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"
	"github.com/zuoyebang/bitalostored/stored/server"

	"github.com/zuoyebang/bitalostored/stored/internal/log"

	"github.com/spf13/pflag"
)

func main() {
	configFile := pflag.String("conf.file", "conf/dbconfig.toml", "please input the dbconfig file")
	serverAddr := pflag.String("server.address", "", "please input the listen address")
	raftNodeId := pflag.Uint64("raft.node.id", 0, "please input the raft node id")
	clusterId := pflag.Uint64("raft.cluster.id", 0, "please input the raft cluster id")
	pflag.Parse()

	if err := config.GlobalConfig.LoadFromFile(*configFile, *serverAddr, *raftNodeId, *clusterId); err != nil {
		panic(fmt.Sprintf("load global config failed err:%s", err.Error()))
	}

	log.NewLogger(&log.Options{
		IsDebug:      config.GlobalConfig.Log.IsDebug,
		RotationTime: config.GlobalConfig.Log.RotationTime,
		LogPath:      config.GetBitalosLogPath(),
	})

	tclock.InitTimeClock()

	log.Infof("create server with config\n%s", config.GlobalConfig)

	startPprof()

	s, err := server.NewServer()
	if err != nil {
		log.Errorf("new server fail err:%s", err.Error())
		os.Exit(1)
	}

	log.Info("server is working ...")

	server.InitLuaPool(s)
	raft.RaftInit(s)
	server.RunInfoCollection(s)
	raft.RaftStart(s)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go s.ListenAndServe()

	<-sc

	defer log.CloseLog()

	log.Info("server is closing ...")
	s.Close()
	log.Info("server is closed ...")
}

func startPprof() {
	if !config.GlobalConfig.Plugin.OpenPprof {
		return
	}

	go func() {
		pprofAddr := config.GlobalConfig.Plugin.PprofAddr
		if err := http.ListenAndServe(pprofAddr, nil); err != nil {
			log.Fatal(err)
		}
	}()
}
