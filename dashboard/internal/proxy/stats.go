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
	"time"

	"go.uber.org/atomic"
)

type Stats struct {
	Online bool `json:"online"`
	Closed bool `json:"closed"`

	ReadCrossCloud bool `json:"read_cross_cloud"`
	PoolActive     int  `json:"pool_active"`

	CmdOps struct {
		Total       int64         `json:"total"`
		Fails       int64         `json:"fails"`
		PeriodFails int64         `json:"periodfails"`
		QPS         int64         `json:"qps"`
		Cmd         []*CalOpStats `json:"cmd,omitempty"`

		OpsCost
	} `json:"cdm_ops"`

	Pool PoolStat `json:"pool"`

	Sessions struct {
		Total int64 `json:"total"`
		Alive int64 `json:"alive"`
	} `json:"sessions"`

	Rusage struct {
		Now string  `json:"now"`
		CPU float64 `json:"cpu"`
		Mem int64   `json:"mem"`
		Raw *Usage  `json:"raw,omitempty"`
	} `json:"rusage"`

	Runtime *RuntimeStats `json:"runtime,omitempty"`

	infos  [2][]byte
	rIndex int
}

type Usage struct {
	Utime time.Duration `json:"utime"`
	Stime time.Duration `json:"stime"`

	MaxRss int64 `json:"max_rss"`
	Ixrss  int64 `json:"ix_rss"`
	Idrss  int64 `json:"id_rss"`
	Isrss  int64 `json:"is_rss"`
}

type PoolStat struct {
	ActiveCount int `json:"active_count"`
	IdleCount   int `json:"idle_count"`
}

type CalOpStats struct {
	OpStr        string `json:"opstr"`
	Calls        int64  `json:"calls"`
	Usecs        int64  `json:"usecs"`
	UsecsPercall int64  `json:"usecs_percall"`
	Fails        int64  `json:"fails"`
	PeriodFails  int64  `json:"periodfails"`
}

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
