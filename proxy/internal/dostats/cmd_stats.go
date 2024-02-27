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

package dostats

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CmdServer CmdType = "cmd_proxy"
)

var CmdStats = map[CmdType]*docalOpStats{
	CmdServer: &docalOpStats{
		opmap: make(map[string]*OpStats, 64),
	},
}

type CmdType string

type docalOpStats struct {
	sync.RWMutex

	opmap       map[string]*OpStats
	total       atomic.Int64
	fails       atomic.Int64
	periodfails atomic.Int64
	qps         atomic.Int64
}

func init() {
	go func() {
		for {
			stat := CmdStats[CmdServer]
			total := stat.total.Load()
			time.Sleep(time.Second * 2)
			delta := stat.total.Load() - total
			normalized := math.Max(0, float64(delta)) + 0.5
			stat.qps.Store(int64(normalized / 2))
		}
	}()
}

func OpTotal(ct CmdType) int64 {
	return CmdStats[ct].total.Load()
}

func OpFails(ct CmdType) int64 {
	return CmdStats[ct].fails.Load()
}

func OpPeriodFails(ct CmdType) int64 {
	return CmdStats[ct].periodfails.Load()
}

func OpQPS(ct CmdType) int64 {
	return CmdStats[ct].qps.Load()
}

func getOpStats(ct CmdType, opstr string, create bool) (*OpStats, error) {
	CmdStats[ct].RLock()
	s := CmdStats[ct].opmap[opstr]
	CmdStats[ct].RUnlock()

	if s != nil || !create {
		return s, nil
	}

	CmdStats[ct].Lock()
	s = CmdStats[ct].opmap[opstr]
	if s == nil {
		s = &OpStats{Opstr: opstr}
		CmdStats[ct].opmap[opstr] = s
	}
	CmdStats[ct].Unlock()
	return s, nil
}

func GetOpStatsAll(ct CmdType) ([]*CalOpStats, error) {
	var all = make([]*CalOpStats, 0, 128)
	CmdStats[ct].RLock()
	for _, s := range CmdStats[ct].opmap {
		all = append(all, s.OpStats())
	}
	CmdStats[ct].RUnlock()
	sort.Sort(sliceCalOpStats(all))
	return all, nil
}

func ResetStats() {
	for ct, _ := range CmdStats {
		CmdStats[ct].Lock()
		CmdStats[ct].opmap = make(map[string]*OpStats, 128)
		CmdStats[ct].Unlock()

		CmdStats[ct].total.Store(0)
		CmdStats[ct].fails.Store(0)
		CmdStats[ct].periodfails.Store(0)
	}
}

func PeriodResetStats() {
	for ct, _ := range CmdStats {
		CmdStats[ct].Lock()
		CmdStats[ct].opmap = make(map[string]*OpStats, 128)
		CmdStats[ct].Unlock()
		CmdStats[ct].periodfails.Store(0)
	}
}

func IncrOpTotal(ct CmdType, n int64) {
	CmdStats[ct].total.Add(n)
}

func IncrOpStats(ct CmdType, e *OpStats) {
	s, _ := getOpStats(ct, e.Opstr, true)
	s.Calls.Add(e.Calls.Swap(0))
	s.Nsecs.Add(e.Nsecs.Swap(0))
	if n := e.Fails.Swap(0); n != 0 {
		s.Fails.Add(n)
		CmdStats[ct].fails.Add(n)
	}
	if n := e.PeriodFails.Swap(0); n != 0 {
		s.PeriodFails.Add(n)
		CmdStats[ct].periodfails.Add(n)
	}
}
