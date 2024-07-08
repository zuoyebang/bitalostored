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

package server

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/trycatch"
)

type cpuAdjust struct {
	periodPath string
	quotaPath  string
	lastCores  int
	optCores   int
}

func RunCpuAdjuster(s *Server) {
	var addr string
	if len(s.laddr) > 1 {
		addr = s.laddr[1:]
	}
	path := fmt.Sprintf("/sys/fs/cgroup/cpu/stored/server_%s_%s", config.GlobalConfig.Server.ProductName, addr)
	log.Infof("cpu cgroup base path %s", path)

	c := &cpuAdjust{
		periodPath: filepath.Join(path, "cpu.cfs_period_us"),
		quotaPath:  filepath.Join(path, "cpu.cfs_quota_us"),
	}

	if config.GlobalConfig.Server.Maxprocs > 1 {
		c.optCores = config.GlobalConfig.Server.Maxprocs / 2
	}

	c.setGoMaxProcs()

	go func() {
		for {
			if s.IsClosed() {
				return
			}

			c.setGoMaxProcs()
			s.Info.RuntimeStats.NumProcs = c.lastCores * 2
			time.Sleep(60 * time.Second)
		}
	}()
}

func (c *cpuAdjust) setGoMaxProcs() {
	defer func() {
		trycatch.Panic("cpuAdjust", recover())
	}()

	cores := c.getCpuNum()
	if cores == 0 && c.optCores > 0 {
		cores = c.optCores
	}
	if cores < config.MinCores {
		log.Warnf("cpu procs less than(%d). num: %d", config.MinCores, cores)
		cores = config.MinCores
	}
	if cores > config.MaxCores {
		log.Warnf("cpu procs exceed limit(%d). num: %d", config.MaxCores, cores)
		cores = config.MaxCores
	}
	if cores != c.lastCores && cores > 0 {
		runtime.GOMAXPROCS(cores * 2)
		log.Infof("cpu procs change: %d => %d, GOMAXPROCS: %d => %d", c.lastCores, cores, c.lastCores*2, cores*2)
		c.lastCores = cores
	}
}

func (c *cpuAdjust) getCpuNum() int {
	periodInt, _ := readCpuInfo(c.periodPath)
	quotaInt, _ := readCpuInfo(c.quotaPath)

	if quotaInt == 0 || periodInt == 0 {
		return 0
	}

	return int(quotaInt / periodInt)
}

func readCpuInfo(path string) (num int64, err error) {
	content, err := os.ReadFile(path)
	if err != nil || len(content) <= 0 {
		return 0, err
	}
	return strconv.ParseInt(string(bytes.TrimSpace(content)), 10, 64)
}
