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

package utils

import (
	"runtime"
	"sync/atomic"
	"time"
)

type SysUsage struct {
	*Usage
	Now      time.Time
	CPU      float64
	MemStats *runtime.MemStats
}

var lastSysUsage atomic.Value

func init() {
	initSysUsage()
	go func() {
		for {
			time.Sleep(4 * time.Second)
			initSysUsage()
		}
	}()
}

func initSysUsage() {
	cpu, usage, err := CPUUsage(time.Second)
	var r runtime.MemStats
	runtime.ReadMemStats(&r)
	if err != nil {
		lastSysUsage.Store(&SysUsage{
			Now:      time.Now(),
			MemStats: &r,
		})
	} else {
		lastSysUsage.Store(&SysUsage{
			Now:      time.Now(),
			CPU:      cpu,
			MemStats: &r,
			Usage:    usage,
		})
	}
}

func GetSysUsage() *SysUsage {
	if p := lastSysUsage.Load(); p != nil {
		return p.(*SysUsage)
	}
	return nil
}
