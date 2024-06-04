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

package proxy

import (
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalostored/proxy/internal/dostats"
	"github.com/zuoyebang/bitalostored/proxy/internal/switcher"
	"github.com/zuoyebang/bitalostored/proxy/internal/utils"
	"github.com/zuoyebang/bitalostored/proxy/resp"
	"github.com/zuoyebang/bitalostored/proxy/router"
)

var commandDistributor map[string]int
var doStatsLastTime time.Time = time.Now()
var proxyStat = NewStats()
var simpleStat = NewStats()

func init() {
	commandDistributor = make(map[string]int, 0)
	commandDistributor = map[string]int{
		resp.GET:    5,
		resp.SET:    5,
		resp.SETEX:  5,
		resp.SETNX:  5,
		resp.GETSET: 5,
		resp.INCR:   5,
		resp.INCRBY: 5,
		resp.DECR:   5,
		resp.DECRBY: 5,
		resp.STRLEN: 5,
		resp.MGET:   5,

		resp.HSET:    1,
		resp.HMSET:   1,
		resp.HGET:    1,
		resp.HMGET:   1,
		resp.HLEN:    1,
		resp.HINCRBY: 1,
		resp.HVALS:   1,
		resp.HGETALL: 1,
		resp.HSCAN:   1,

		resp.SADD:      2,
		resp.SREM:      2,
		resp.SCARD:     2,
		resp.SISMEMBER: 2,
		resp.SPOP:      2,
		resp.SMEMBERS:  2,
		resp.SSCAN:     2,

		resp.ZADD:             3,
		resp.ZSCORE:           3,
		resp.ZRANGEBYLEX:      3,
		resp.ZCOUNT:           3,
		resp.ZRANGE:           3,
		resp.ZRANGEBYSCORE:    3,
		resp.ZRANK:            3,
		resp.ZREM:             3,
		resp.ZINCRBY:          3,
		resp.ZREMRANGEBYLEX:   3,
		resp.ZREMRANGEBYRANK:  3,
		resp.ZREMRANGEBYSCORE: 3,
		resp.ZREVRANGE:        3,
		resp.ZREVRANGEBYSCORE: 3,
		resp.ZREVRANK:         3,
		resp.ZLEXCOUNT:        3,
		resp.ZSCAN:            3,

		resp.LPOP:    4,
		resp.LPUSH:   4,
		resp.LPUSHX:  4,
		resp.LTRIM:   4,
		resp.RPUSH:   4,
		resp.RPOP:    4,
		resp.RPUSHX:  4,
		resp.LLEN:    4,
		resp.LINDEX:  4,
		resp.LREM:    4,
		resp.LRANGE:  4,
		resp.LINSERT: 4,
		resp.LSET:    4,
	}
}

type Stats struct {
	Online bool `json:"online"`
	Closed bool `json:"closed"`

	ReadCrossCloud bool `json:"read_cross_cloud"`
	PoolActive     int  `json:"pool_active"`

	CmdOps struct {
		Total       int64                 `json:"total"`
		Fails       int64                 `json:"fails"`
		PeriodFails int64                 `json:"periodfails"`
		QPS         int64                 `json:"qps"`
		Cmd         []*dostats.CalOpStats `json:"cmd,omitempty"`

		OpsCost
	} `json:"cdm_ops"`

	Pool dostats.PoolStat `json:"pool"`

	Sessions struct {
		Total int64 `json:"total"`
		Alive int64 `json:"alive"`
	} `json:"sessions"`

	Rusage struct {
		Now string       `json:"now"`
		CPU float64      `json:"cpu"`
		Mem int64        `json:"mem"`
		Raw *utils.Usage `json:"raw,omitempty"`
	} `json:"rusage"`

	Runtime *RuntimeStats `json:"runtime,omitempty"`

	infos  [2][]byte
	rIndex int
}

var goc = GlobalOpsCost{}

type GlobalOpsCost struct {
	AvgCost   atomic.Int64 `json:"avg_cost"`
	KVCost    atomic.Int64 `json:"kv_cost"`
	ListCost  atomic.Int64 `json:"list_cost"`
	HashCost  atomic.Int64 `json:"hash_cost"`
	SetCost   atomic.Int64 `json:"set_cost"`
	ZsetCost  atomic.Int64 `json:"zset_cost"`
	WriteCost atomic.Int64 `json:"write_cost"`
	ReadCost  atomic.Int64 `json:"read_cost"`
}

type OpsCost struct {
	AvgCost   int64 `json:"avg_cost"`
	KVCost    int64 `json:"kv_cost"`
	ListCost  int64 `json:"list_cost"`
	HashCost  int64 `json:"hash_cost"`
	SetCost   int64 `json:"set_cost"`
	ZsetCost  int64 `json:"zset_cost"`
	WriteCost int64 `json:"write_cost"`
	ReadCost  int64 `json:"read_cost"`
}

func NewStats() *Stats {
	s := &Stats{}
	s.infos[0] = make([]byte, 0, 1024)
	s.infos[1] = make([]byte, 0, 1024)
	s.rIndex = 0
	return s
}

func (stats *Stats) getInfo() []byte {
	wIndex := (stats.rIndex + 1) % 2

	stats.infos[wIndex] = stats.infos[wIndex][:0]
	stats.infos[wIndex] = append(stats.infos[wIndex], []byte("# Status\nstatus: true \n")...)
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "cdm_ops_total:", stats.CmdOps.Total)
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "cdm_ops_fails:", stats.CmdOps.Fails)
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "cdm_ops_qps:", stats.CmdOps.QPS)
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "cmd_cost_avg:", stats.CmdOps.AvgCost)
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "cmd_cost_kv:", stats.CmdOps.KVCost)
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "cmd_cost_list:", stats.CmdOps.ListCost)
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "cmd_cost_hash:", stats.CmdOps.HashCost)
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "cmd_cost_set:", stats.CmdOps.SetCost)
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "cmd_cost_zset:", stats.CmdOps.ZsetCost)
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "cmd_cost_write:", stats.CmdOps.WriteCost)
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "cmd_cost_read:", stats.CmdOps.ReadCost)

	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "sessions_total:", stats.Sessions.Total)
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "sessions_alive:", stats.Sessions.Alive)
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "rusage_mem:", stats.Rusage.Mem)
	stats.infos[wIndex] = appendInfoFloat(stats.infos[wIndex], "rusage_cpu:", stats.Rusage.CPU, 4)
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "runtime_gc_num:", int64(stats.Runtime.GC.Num))
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "runtime_gc_total_pausems:", int64(stats.Runtime.GC.TotalPauseMs))
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "runtime_num_procs:", int64(stats.Runtime.NumProcs))
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "runtime_num_goroutines:", int64(stats.Runtime.NumGoroutines))
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "runtime_num_cgo_call:", int64(stats.Runtime.NumCgoCall))
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "pool_active_count:", int64(stats.Pool.ActiveCount))
	stats.infos[wIndex] = appendInfoInt(stats.infos[wIndex], "pool_idle_count:", int64(stats.Pool.IdleCount))
	stats.infos[wIndex] = append(stats.infos[wIndex], '\n')

	stats.rIndex = wIndex
	return stats.infos[stats.rIndex]
}

func appendInfoInt(buf []byte, key string, value int64) []byte {
	buf = append(buf, key...)
	buf = strconv.AppendInt(buf, value, 10)
	buf = append(buf, '\n')
	return buf
}

func appendInfoFloat(buf []byte, key string, value float64, prec int) []byte {
	buf = append(buf, key...)
	buf = strconv.AppendFloat(buf, value, 'f', prec, 64)
	buf = append(buf, '\n')
	return buf
}

type RuntimeStats struct {
	General struct {
		Alloc   uint64 `json:"alloc"`
		Sys     uint64 `json:"sys"`
		Lookups uint64 `json:"lookups"`
		Mallocs uint64 `json:"mallocs"`
		Frees   uint64 `json:"frees"`
	} `json:"general"`

	Heap struct {
		Alloc   uint64 `json:"alloc"`
		Sys     uint64 `json:"sys"`
		Idle    uint64 `json:"idle"`
		Inuse   uint64 `json:"inuse"`
		Objects uint64 `json:"objects"`
	} `json:"heap"`

	GC struct {
		Num          uint32  `json:"num"`
		CPUFraction  float64 `json:"cpu_fraction"`
		TotalPauseMs uint64  `json:"total_pausems"`
	} `json:"gc"`

	NumProcs      int   `json:"num_procs"`
	NumGoroutines int   `json:"num_goroutines"`
	NumCgoCall    int64 `json:"num_cgo_call"`
}

type StatsFlags uint32

func (s StatsFlags) HasBit(m StatsFlags) bool {
	return (s & m) != 0
}

const (
	StatsCmds = StatsFlags(1 << iota)
	StatsSlots
	StatsRuntime

	StatsFull = StatsFlags(^uint32(0))
)

func GetStats(s *Proxy, update bool) *Stats {
	if !update {
		return proxyStat
	}

	stats := proxyStat
	stats.Online = s.IsOnline()
	stats.Closed = s.IsClosed()
	stats.ReadCrossCloud = switcher.ReadCrossCloud.Load()
	stats.PoolActive = dostats.GetPoolActive()

	stats.CmdOps.Total = dostats.OpTotal(dostats.CmdServer)
	stats.CmdOps.Fails = dostats.OpFails(dostats.CmdServer)
	stats.CmdOps.PeriodFails = dostats.OpPeriodFails(dostats.CmdServer)
	stats.CmdOps.QPS = dostats.OpQPS(dostats.CmdServer)

	var newStats []*dostats.CalOpStats
	newStats, _ = dostats.GetOpStatsAll(dostats.CmdServer)
	stats.CmdOps.Cmd = newStats

	nowTime := time.Now()
	if nowTime.Unix()-doStatsLastTime.Unix() >= 10 {
		doStatsLastTime = nowTime
		stats.CmdOps.OpsCost = getOpsCostFromCommandCost(newStats)
	} else {
		stats.CmdOps.OpsCost = getOpsCostFromCache()
	}

	stats.Sessions.Total = dostats.ConnsTotal()
	stats.Sessions.Alive = dostats.ConnsAlive()

	ps := dostats.GetPoolStat()
	stats.Pool.ActiveCount = ps.ActiveCount
	stats.Pool.IdleCount = ps.IdleCount

	if u := dostats.GetSysUsage(); u != nil {
		stats.Rusage.Now = u.Now.String()
		stats.Rusage.CPU = u.CPU
		stats.Rusage.Mem = u.MemTotal()
		stats.Rusage.Raw = u.Usage
	}

	r := dostats.GetMemUsage()
	stats.Runtime = &RuntimeStats{}
	stats.Runtime.General.Alloc = r.Alloc
	stats.Runtime.General.Sys = r.Sys
	stats.Runtime.General.Lookups = r.Lookups
	stats.Runtime.General.Mallocs = r.Mallocs
	stats.Runtime.General.Frees = r.Frees
	stats.Runtime.Heap.Alloc = r.HeapAlloc
	stats.Runtime.Heap.Sys = r.HeapSys
	stats.Runtime.Heap.Idle = r.HeapIdle
	stats.Runtime.Heap.Inuse = r.HeapInuse
	stats.Runtime.Heap.Objects = r.HeapObjects
	stats.Runtime.GC.Num = r.NumGC
	stats.Runtime.GC.CPUFraction = r.GCCPUFraction
	stats.Runtime.GC.TotalPauseMs = r.PauseTotalNs / uint64(time.Millisecond)
	stats.Runtime.NumProcs = runtime.GOMAXPROCS(0)
	stats.Runtime.NumGoroutines = runtime.NumGoroutine()
	stats.Runtime.NumCgoCall = runtime.NumCgoCall()

	SetProxyInfo(stats)
	stats.Copy(simpleStat)

	return stats
}

func (stats *Stats) Copy(s *Stats) {
	s.Online = stats.Online
	s.Closed = stats.Closed
	s.ReadCrossCloud = stats.ReadCrossCloud
	s.PoolActive = stats.PoolActive

	s.CmdOps.Total = stats.CmdOps.Total
	s.CmdOps.Fails = stats.CmdOps.Fails
	s.CmdOps.PeriodFails = stats.CmdOps.PeriodFails
	s.CmdOps.QPS = stats.CmdOps.QPS

	s.CmdOps.OpsCost = getOpsCostFromCache()

	s.Sessions.Total = stats.Sessions.Total
	s.Sessions.Alive = stats.Sessions.Alive

	s.Pool.ActiveCount = stats.Pool.ActiveCount
	s.Pool.IdleCount = stats.Pool.IdleCount

	s.Rusage = stats.Rusage
	s.Runtime = stats.Runtime
}

func GetSimpleStats() *Stats {
	return simpleStat
}

func getOpsCostFromCache() OpsCost {
	return OpsCost{
		AvgCost:   goc.AvgCost.Load(),
		KVCost:    goc.KVCost.Load(),
		ListCost:  goc.ListCost.Load(),
		HashCost:  goc.HashCost.Load(),
		SetCost:   goc.SetCost.Load(),
		ZsetCost:  goc.ZsetCost.Load(),
		WriteCost: goc.WriteCost.Load(),
		ReadCost:  goc.ReadCost.Load(),
	}
}

func getOpsCostFromCommandCost(newCalStats []*dostats.CalOpStats) OpsCost {
	var cc []callCost
	cc = make([]callCost, 8)
	for _, s := range newCalStats {
		index := commandIndex(s.OpStr)
		cc[index].call = cc[index].call + s.Calls
		cc[index].cost = cc[index].cost + s.Usecs
		if router.IsWriteCmd(s.OpStr) {
			cc[6].call = cc[6].call + s.Calls
			cc[6].cost = cc[6].cost + s.Usecs
		} else {
			cc[7].call = cc[7].call + s.Calls
			cc[7].cost = cc[7].cost + s.Usecs
		}
	}
	for i := 1; i < len(cc); i++ {
		cc[0].call = cc[0].call + cc[i].call
		cc[0].cost = cc[0].cost + cc[i].cost
	}
	var avgCost, hashCost, setCost, zsetCost, listCost, kvCost, writeCost, readCost int64
	if cc[0].call != 0 {
		avgCost = cc[0].cost / cc[0].call
	}
	if cc[1].call != 0 {
		hashCost = cc[1].cost / cc[1].call
	}
	if cc[2].call != 0 {
		setCost = cc[2].cost / cc[2].call
	}
	if cc[3].call != 0 {
		zsetCost = cc[3].cost / cc[3].call
	}
	if cc[4].call != 0 {
		listCost = cc[4].cost / cc[4].call
	}
	if cc[5].call != 0 {
		kvCost = cc[5].cost / cc[5].call
	}
	if cc[6].call != 0 {
		writeCost = cc[6].cost / cc[6].call
	}
	if cc[7].call != 0 {
		readCost = cc[7].cost / cc[7].call
	}

	goc.AvgCost.Store(avgCost)
	goc.HashCost.Store(hashCost)
	goc.SetCost.Store(setCost)
	goc.ZsetCost.Store(zsetCost)
	goc.ListCost.Store(listCost)
	goc.KVCost.Store(kvCost)
	goc.WriteCost.Store(writeCost)
	goc.ReadCost.Store(readCost)

	return getOpsCostFromCache()
}

type callCost struct {
	call int64
	cost int64
}

func commandIndex(command string) int {
	if commandDistributor == nil {
		return 0
	}
	index, ok := commandDistributor[command]
	if !ok {
		return 0
	}
	return index
}
