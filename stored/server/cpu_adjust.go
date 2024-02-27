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

package server

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

const cpuProcMax = 32

type cpuAdjust struct {
	path       string
	periodPath string
	quotaPath  string
	lastCpuNum int
}

func NewCpuAdjust(path string, lastCpuNum int) *cpuAdjust {
	c := &cpuAdjust{}

	log.Infof("cpu cgroup base path: %s", path)
	c.periodPath = filepath.Join(path, "cpu.cfs_period_us")
	c.quotaPath = filepath.Join(path, "cpu.cfs_quota_us")
	c.lastCpuNum = lastCpuNum

	return c
}

func (c *cpuAdjust) Run(s *Server) {
	var cpuNum int
	go func() {
		for {
			cpuNum = c.getCpuNum()
			if cpuNum > cpuProcMax {
				log.Warnf("cpu procs exceed limit. num: %d", cpuNum)
				cpuNum = cpuProcMax
			}
			if cpuNum != c.lastCpuNum && cpuNum > 0 {
				runtime.GOMAXPROCS(cpuNum)
				log.Infof("cpu procs change. %d => %d", c.lastCpuNum, cpuNum)
				c.lastCpuNum = cpuNum
			}
			s.Info.RuntimeStats.NumProcs = cpuNum
			time.Sleep(60 * time.Second)
		}
	}()
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
