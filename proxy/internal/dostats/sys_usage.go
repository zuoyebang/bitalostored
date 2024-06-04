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

package dostats

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalostored/proxy/internal/utils"
)

type SysUsage struct {
	Now time.Time
	CPU float64
	*utils.Usage
}

var lastMemUsage atomic.Value
var lastSysUsage atomic.Value

func init() {
	go func() {
		for {
			r := &runtime.MemStats{}
			runtime.ReadMemStats(r)
			lastMemUsage.Store(r)

			cpu, usage, err := utils.CPUUsage(time.Second)
			if err != nil {
				lastSysUsage.Store(&SysUsage{
					Now: time.Now(),
				})
			} else {
				lastSysUsage.Store(&SysUsage{
					Now: time.Now(),
					CPU: cpu, Usage: usage,
				})
			}

			time.Sleep(time.Second * 5)
		}
	}()
}

func GetSysUsage() *SysUsage {
	if p := lastSysUsage.Load(); p != nil {
		return p.(*SysUsage)
	}
	return nil
}

func GetMemUsage() *runtime.MemStats {
	if p := lastMemUsage.Load(); p != nil {
		return p.(*runtime.MemStats)
	}
	return nil
}
