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

package slowshield

import (
	"container/heap"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalostored/stored/internal/resp"

	"github.com/zuoyebang/bitalostored/stored/internal/config"

	"github.com/zuoyebang/bitalostored/stored/internal/log"

	"github.com/zuoyebang/bitalostored/butils/timesize"

	"github.com/zuoyebang/bitalostored/butils/unsafe2"
)

type SlowShield struct {
	mu                sync.RWMutex
	isOpen            bool
	ttl               timesize.Duration
	slowTime          timesize.Duration
	keySlowWindowTime timesize.Duration
	maxAllowSlowTime  time.Duration
	maxExec           int
	topN              int
	totalSlowTime     atomic.Int64
	slowKey           map[string]int64
	topSlowKey        map[string]int64
}

var notCheckCmd = map[string]bool{
	resp.INFO: true,
}

func NewSlowShield() *SlowShield {
	sc := &SlowShield{
		isOpen: config.GlobalConfig.Server.SlowShield,
	}
	sc.adjustByGlobalConfig()
	if sc.isOpen {
		sc.doStats()
	}
	return sc
}

func (sc *SlowShield) adjustByGlobalConfig() {
	sc.slowKey = make(map[string]int64, 32)
	sc.topSlowKey = make(map[string]int64, 16)
	sc.isOpen = config.GlobalConfig.Server.SlowShield

	if config.GlobalConfig.Server.SlowTTL < timesize.Duration(1*time.Second) {
		sc.ttl = timesize.Duration(1 * time.Second)
	} else {
		sc.ttl = config.GlobalConfig.Server.SlowTTL
	}
	if config.GlobalConfig.Server.SlowMaxExec < 100 {
		sc.maxExec = 100
	} else {
		sc.maxExec = config.GlobalConfig.Server.SlowMaxExec
	}
	if config.GlobalConfig.Server.SlowTopN < 50 {
		sc.topN = 50
	} else {
		sc.topN = config.GlobalConfig.Server.SlowTopN
	}
	if config.GlobalConfig.Server.SlowTime < timesize.Duration(30*time.Millisecond) {
		sc.slowTime = timesize.Duration(30 * time.Millisecond)
	} else {
		sc.slowTime = config.GlobalConfig.Server.SlowTime
	}
	if config.GlobalConfig.Server.SlowKeyWindowTime < timesize.Duration(15*time.Millisecond) {
		sc.keySlowWindowTime = timesize.Duration(15 * time.Millisecond)
	} else {
		sc.keySlowWindowTime = config.GlobalConfig.Server.SlowKeyWindowTime
	}
	sc.maxAllowSlowTime = time.Duration(sc.maxExec) * sc.ttl.Duration()
}

func (sc *SlowShield) CheckSlowShield(cmd string, key []byte) bool {
	if sc.isOpen {
		if len(key) == 0 {
			return false
		}
		sc.mu.RLock()
		defer sc.mu.RUnlock()
		cmdKey := cmd + unsafe2.String(key)
		_, ok := sc.topSlowKey[cmdKey]
		return ok
	}
	return false
}

func (sc *SlowShield) Send(cmd string, key []byte, cost int64) {
	if sc.isOpen {
		if notCheckCmd[cmd] || cost <= 0 {
			return
		}
		cmdKey := cmd + unsafe2.String(key)
		sc.totalSlowTime.Add(cost)
		sc.mu.Lock()
		sc.slowKey[cmdKey] = sc.slowKey[cmdKey] + cost
		sc.mu.Unlock()
	}
}

func (sc *SlowShield) doStats() {
	go func() {
		dostat := func() {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("slow shield dostats panic err:%v stack:%s", r, string(debug.Stack()))
					time.Sleep(200 * time.Millisecond)
				}
			}()

			if sc.totalSlowTime.Load() > sc.maxAllowSlowTime.Nanoseconds() {
				sc.mu.Lock()
				lastSlowKey := sc.slowKey
				totalSlowTime := sc.totalSlowTime.Load()
				sc.slowKey = make(map[string]int64, 32)
				sc.totalSlowTime.Store(0)
				topSlowKey := make(map[string]int64, 16)

				if totalSlowTime > 0 && len(lastSlowKey) > 0 {
					pq := make(PriorityQueue, 0, 10)
					index := 0
					for key, slowKeyCost := range lastSlowKey {
						pq.PushTopN(&Item{
							value:    key,
							priority: slowKeyCost,
						}, sc.topN)
						index++
					}
					heap.Init(&pq)

					for _, it := range pq {
						keyCostMs := it.priority / 1e6
						if keyCostMs > sc.keySlowWindowTime.Duration().Milliseconds() {
							log.Infof("slow shield [cmdkey:%q] [slowtime:%dms]", it.value, keyCostMs)
							topSlowKey[it.value] = it.priority
						}
					}
				}
				sc.topSlowKey = topSlowKey
				sc.mu.Unlock()
			} else {
				sc.mu.Lock()
				sc.slowKey = make(map[string]int64, 32)
				sc.topSlowKey = make(map[string]int64, 16)
				sc.totalSlowTime.Store(0)
				sc.mu.Unlock()
			}
		}

		for {
			dostat()
			time.Sleep(sc.ttl.Duration())
		}
	}()
}
