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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb"
	"github.com/zuoyebang/bitalostored/stored/internal/bytepools"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"

	"github.com/zuoyebang/bitalostored/butils"
)

type SInfo struct {
	Server         SinfoServer
	Client         SinfoClient
	Cluster        SinfoCluster
	Stats          SinfoStats
	Data           SinfoData
	RuntimeStats   SRuntimeStats
	BitalosdbUsage *bitsdb.BitsUsage
}

func (sinfo *SInfo) Marshal() ([]byte, func()) {
	var pos int = 0
	buf, closer := bytepools.BytePools.GetBytePool(8192)
	pos += sinfo.Server.AppendTo(buf, pos)
	pos += sinfo.Client.AppendTo(buf, pos)
	pos += sinfo.Cluster.AppendTo(buf, pos)
	pos += sinfo.Stats.AppendTo(buf, pos)
	pos += sinfo.Data.AppendTo(buf, pos)
	pos += sinfo.BitalosdbUsage.AppendTo(buf, pos)
	pos += sinfo.RuntimeStats.AppendTo(buf, pos)
	return buf[:pos], closer
}

func NewSinfo() *SInfo {
	sinfo := &SInfo{
		Server:         SinfoServer{cache: make([]byte, 0, 2048)},
		Client:         SinfoClient{cache: make([]byte, 0, 256)},
		Cluster:        SinfoCluster{cache: make([]byte, 0, 2048)},
		Stats:          SinfoStats{cache: make([]byte, 0, 2048)},
		Data:           SinfoData{cache: make([]byte, 0, 1024)},
		RuntimeStats:   SRuntimeStats{cache: make([]byte, 0, 3072)},
		BitalosdbUsage: bitsdb.NewBitsUsage(),
	}

	return sinfo
}

type SinfoCluster struct {
	StartModel       ModelType `json:"start_model"`
	Status           bool      `json:"status"`
	Role             string    `json:"role"`
	ClusterId        uint64    `json:"cluster_id"`
	CurrentNodeId    uint64    `json:"current_node_id"`
	RaftAddress      string    `json:"raft_address"`
	LeaderNodeId     uint64    `json:"leader_node_id"`
	LeaderAddress    string    `json:"leader_address"`
	ClusterNodes     string    `json:"cluster_nodes"`
	ClusterNodesList string    `json:"cluster_nodes_list"`

	mutex sync.RWMutex
	cache []byte
}

func (sc *SinfoCluster) Marshal() ([]byte, func()) {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()

	info, closer := bytepools.BytePools.GetBytePool(len(sc.cache))
	num := copy(info[0:], sc.cache)
	return info[:num], closer
}

func (sc *SinfoCluster) AppendTo(target []byte, pos int) int {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()

	return copy(target[pos:], sc.cache)
}

func (sc *SinfoCluster) UpdateCache() {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	sc.cache = sc.cache[:0]

	sc.cache = append(sc.cache, []byte("# ClusterInfo\n")...)
	sc.cache = utils.AppendInfoString(sc.cache, "start_model:", sc.StartModel.String())
	sc.cache = utils.AppendInfoString(sc.cache, "status:", boolToString(sc.Status))
	sc.cache = utils.AppendInfoString(sc.cache, "role:", sc.Role)
	sc.cache = utils.AppendInfoUint(sc.cache, "cluster_id:", sc.ClusterId)
	sc.cache = utils.AppendInfoUint(sc.cache, "current_node_id:", sc.CurrentNodeId)
	sc.cache = utils.AppendInfoString(sc.cache, "raft_address:", sc.RaftAddress)
	sc.cache = utils.AppendInfoUint(sc.cache, "leader_node_id:", sc.LeaderNodeId)
	sc.cache = utils.AppendInfoString(sc.cache, "leader_address:", sc.LeaderAddress)
	sc.cache = utils.AppendInfoString(sc.cache, "cluster_nodes:", sc.ClusterNodes)
	sc.cache = append(sc.cache, sc.ClusterNodesList...)
	sc.cache = append(sc.cache, '\n')
}

type SinfoServer struct {
	MaxProcs      int    `json:"maxprocs"`
	ProcessId     int    `json:"process_id"`
	StartTime     string `json:"start_time"`
	ServerAddress string `json:"server_address"`
	MaxClient     int64  `json:"max_client"`
	SingleDegrade bool   `json:"single_degrade"`
	GitVersion    string `json:"git_version"`
	Compile       string `json:"compile"`
	ConfigFile    string `json:"config_file"`

	mutex sync.RWMutex
	cache []byte
}

func (ss *SinfoServer) Marshal() ([]byte, func()) {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	info, closer := bytepools.BytePools.GetBytePool(len(ss.cache))
	num := copy(info[0:], ss.cache)
	return info[:num], closer
}

func (ss *SinfoServer) AppendTo(target []byte, pos int) int {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	return copy(target[pos:], ss.cache)
}

func (ss *SinfoServer) UpdateCache() {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	ss.cache = ss.cache[:0]
	ss.cache = append(ss.cache, []byte("# Server\n")...)

	ss.cache = utils.AppendInfoInt(ss.cache, "maxprocs:", int64(ss.MaxProcs))
	ss.cache = utils.AppendInfoInt(ss.cache, "process_id:", int64(ss.ProcessId))
	ss.cache = utils.AppendInfoString(ss.cache, "start_time:", ss.StartTime)
	ss.cache = utils.AppendInfoInt(ss.cache, "max_client:", ss.MaxClient)
	ss.cache = utils.AppendInfoString(ss.cache, "single_degrade:", utils.BoolToString(ss.SingleDegrade))
	ss.cache = utils.AppendInfoString(ss.cache, "server_address:", ss.ServerAddress)
	ss.cache = utils.AppendInfoString(ss.cache, "git_version:", ss.GitVersion)
	ss.cache = utils.AppendInfoString(ss.cache, "compile:", ss.Compile)
	ss.cache = utils.AppendInfoString(ss.cache, "config_file:", ss.ConfigFile)
	ss.cache = append(ss.cache, '\n')
}

type SinfoClient struct {
	ClientTotal atomic.Int64 `json:"total_clients"`
	ClientAlive atomic.Int64 `json:"connected_clients"`

	mutex sync.RWMutex
	cache []byte
}

func (sc *SinfoClient) Marshal() ([]byte, func()) {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()

	info, closer := bytepools.BytePools.GetBytePool(len(sc.cache))
	num := copy(info[0:], sc.cache)
	return info[:num], closer
}

func (sc *SinfoClient) UpdateCache() {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	sc.cache = sc.cache[:0]
	sc.cache = append(sc.cache, []byte("# Clients\n")...)
	sc.cache = utils.AppendInfoInt(sc.cache, "total_clients:", sc.ClientTotal.Load())
	sc.cache = utils.AppendInfoInt(sc.cache, "connected_clients:", sc.ClientAlive.Load())
	sc.cache = append(sc.cache, '\n')
}

func (sc *SinfoClient) AppendTo(target []byte, pos int) int {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()

	return copy(target[pos:], sc.cache)
}

type ModelType int
type DbSyncStatusType int

func (mt ModelType) String() string {
	if mt == M_NORMAL {
		return "normal"
	} else if mt == M_OBSERVER {
		return "observer"
	} else if mt == M_WITNESS {
		return "witness"
	}
	return ""
}

func (dst DbSyncStatusType) String() string {
	switch dst {
	case DB_SYNC_SENDING:
		return "data sync sending"
	case DB_SYNC_SEND_FAIL:
		return "data sync send fail"
	case DB_SYNC_SEND_SUCC:
		return "data sync send succ"
	case DB_SYNC_RECVING:
		return "data sync recving"
	case DB_SYNC_RECVING_FAIL:
		return "data sync recv fail"
	case DB_SYNC_RECVING_SUCC:
		return "data sync recv succ"
	case DB_SYNC_PREPARE_FAIL:
		return "data sync prepare fail"
	case DB_SYNC_PREPARE_SUCC:
		return "data sync prepare succ"
	case DB_SYNC_CONN_FAIL:
		return "data sync conn fail"
	case DB_SYNC_CONN_SUCC:
		return "data sync conn succ"
	}
	return ""
}

const (
	M_NORMAL   ModelType = 0
	M_OBSERVER ModelType = 1
	M_WITNESS  ModelType = 2

	DB_SYNC_RUN_TYPE_END  = 0
	DB_SYNC_RUN_TYPE_SEND = 1
	DB_SYNC_RUN_TYPE_RECV = 2

	DB_SYNC_NOTHING      = 0
	DB_SYNC_PREPARE_FAIL = 1
	DB_SYNC_PREPARE_SUCC = 2
	DB_SYNC_SEND_FAIL    = 3
	DB_SYNC_SENDING      = 4
	DB_SYNC_SEND_SUCC    = 5
	DB_SYNC_RECVING_FAIL = 6
	DB_SYNC_RECVING      = 7
	DB_SYNC_RECVING_SUCC = 8
	DB_SYNC_CONN_FAIL    = 9
	DB_SYNC_CONN_SUCC    = 10
)

type SinfoStats struct {
	TotolCmd      atomic.Uint64
	QPS           atomic.Uint64
	QueueLen      int
	RaftLogIndex  uint64
	IsDelExpire   int
	StartModel    ModelType
	DbSyncRunning atomic.Int32
	DbSyncStatus  DbSyncStatusType
	DbSyncErr     string
	IsMigrate     atomic.Int32 `json:"is_migrate"`

	mutex sync.RWMutex
	cache []byte
}

func (ss *SinfoStats) Marshal() ([]byte, func()) {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	info, closer := bytepools.BytePools.GetBytePool(len(ss.cache))
	num := copy(info[0:], ss.cache)
	return info[:num], closer
}

func (ss *SinfoStats) AppendTo(target []byte, pos int) int {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	return copy(target[pos:], ss.cache)
}

func (ss *SinfoStats) UpdateCache() {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	ss.cache = ss.cache[:0]

	ss.cache = append(ss.cache, []byte("# Status\n")...)
	ss.cache = utils.AppendInfoUint(ss.cache, "total_commands_processed:", ss.TotolCmd.Load())
	ss.cache = utils.AppendInfoUint(ss.cache, "instantaneous_ops_per_sec:", ss.QPS.Load())
	ss.cache = utils.AppendInfoUint(ss.cache, "sync_queue_length:", uint64(ss.QueueLen))
	ss.cache = utils.AppendInfoUint(ss.cache, "raft_log_index:", ss.RaftLogIndex)
	ss.cache = utils.AppendInfoInt(ss.cache, "is_del_expire:", int64(ss.IsDelExpire))
	ss.cache = utils.AppendInfoInt(ss.cache, "is_migrate:", int64(ss.IsMigrate.Load()))
	ss.cache = utils.AppendInfoInt(ss.cache, "db_sync_running:", int64(ss.DbSyncRunning.Load()))
	ss.cache = utils.AppendInfoString(ss.cache, "db_sync_status:", ss.DbSyncStatus.String())
	ss.cache = utils.AppendInfoString(ss.cache, "db_sync_err:", ss.DbSyncErr)
	ss.cache = append(ss.cache, '\n')
}

func boolToString(ok bool) string {
	if ok {
		return "true"
	} else {
		return "false"
	}
}

type SinfoData struct {
	UsedSize         int64 `json:"used_size"`
	DataSize         int64 `json:"data_size"`
	RaftNodeHostSize int64 `json:"raft_nodehost_size"`
	RaftWalSize      int64 `json:"raft_wal_size"`
	SnapshotSize     int64 `json:"snapshot_size"`

	mutex sync.RWMutex
	cache []byte
}

func (sd *SinfoData) Samples() {
	sd.UsedSize = butils.GetDirSize(config.GetBitalosDbPath())
	sd.DataSize = butils.GetDirSize(config.GetBitalosDbDataPath())
	sd.RaftNodeHostSize = butils.GetDirSize(config.GetBitalosRaftNodeHostPath())
	sd.RaftWalSize = butils.GetDirSize(config.GetBitalosRaftWalPath())
	sd.SnapshotSize = butils.GetDirSize(config.GetBitalosSnapshotPath())

	sd.UpdateCache()
}

func (sd *SinfoData) Marshal() ([]byte, func()) {
	sd.mutex.RLock()
	defer sd.mutex.RUnlock()

	info, closer := bytepools.BytePools.GetBytePool(len(sd.cache))
	num := copy(info[0:], sd.cache)
	return info[:num], closer
}

func (sd *SinfoData) AppendTo(target []byte, pos int) int {
	sd.mutex.RLock()
	defer sd.mutex.RUnlock()

	return copy(target[pos:], sd.cache)
}

func (sd *SinfoData) UpdateCache() {
	sd.mutex.Lock()
	defer sd.mutex.Unlock()

	sd.cache = sd.cache[:0]
	sd.cache = append(sd.cache, []byte("# Data\n")...)

	sd.cache = utils.AppendInfoInt(sd.cache, "disk_used_size:", sd.UsedSize)
	sd.cache = utils.AppendInfoString(sd.cache, "disk_used_fmt_size:", butils.FmtSize(uint64(sd.UsedSize)))
	sd.cache = utils.AppendInfoInt(sd.cache, "disk_data_size:", sd.DataSize)
	sd.cache = utils.AppendInfoString(sd.cache, "disk_data_fmt_size:", butils.FmtSize(uint64(sd.DataSize)))
	sd.cache = utils.AppendInfoInt(sd.cache, "disk_raft_nodehost_size:", sd.RaftNodeHostSize)
	sd.cache = utils.AppendInfoInt(sd.cache, "disk_raft_wal_size:", sd.RaftWalSize)
	sd.cache = utils.AppendInfoInt(sd.cache, "disk_snapshot_size:", sd.SnapshotSize)

	sd.cache = utils.AppendInfoInt(sd.cache, "bithash_compression_type:", int64(config.GlobalConfig.Bitalos.BithashCompressionType))
	sd.cache = utils.AppendInfoString(sd.cache, "cache_fmt_size:", butils.FmtSize(uint64(config.GlobalConfig.Bitalos.CacheSize.Int64())))
	sd.cache = utils.AppendInfoString(sd.cache, "enable_wal:", boolToString(config.GlobalConfig.Bitalos.EnableWAL))
	sd.cache = utils.AppendInfoString(sd.cache, "enable_raftlog_restore:", boolToString(config.GlobalConfig.Bitalos.EnableRaftlogRestore))

	sd.cache = append(sd.cache, '\n')
}

type SRuntimeStats struct {
	General struct {
		Alloc   uint64 `json:"runtime_general_alloc"`
		Sys     uint64 `json:"runtime_general_sys"`
		Lookups uint64 `json:"runtime_general_lookups"`
		Mallocs uint64 `json:"runtime_general_mallocs"`
		Frees   uint64 `json:"runtime_general_frees"`
	} `json:"runtime_general"`

	Heap struct {
		Alloc   uint64 `json:"runtime_heap_alloc"`
		Sys     uint64 `json:"runtime_heap_sys"`
		Idle    uint64 `json:"runtime_heap_idle"`
		Inuse   uint64 `json:"runtime_heap_inuse"`
		Objects uint64 `json:"runtime_heap_objects"`
	} `json:"heap"`

	GC struct {
		Num          uint32  `json:"runtime_gc_num"`
		CPUFraction  float64 `json:"runtime_gc_cpu_fraction"`
		TotalPauseMs uint64  `json:"runtime_gc_total_pausems"`
	} `json:"gc"`

	NumProcs      int `json:"runtime_num_procs"`
	NumGoroutines int `json:"runtime_num_goroutines"`

	MemoryTotal int64   `json:"memory_total"`
	MemoryShr   int64   `json:"memory_shr"`
	CPU         float64 `json:"cpu"`

	mutex sync.RWMutex
	cache []byte
}

func (srs *SRuntimeStats) Marshal() ([]byte, func()) {
	srs.mutex.RLock()
	defer srs.mutex.RUnlock()

	info, closer := bytepools.BytePools.GetBytePool(len(srs.cache))
	num := copy(info[0:], srs.cache)
	return info[:num], closer
}

func (srs *SRuntimeStats) AppendTo(target []byte, pos int) int {
	srs.mutex.RLock()
	defer srs.mutex.RUnlock()

	return copy(target[pos:], srs.cache)
}

func (srs *SRuntimeStats) UpdateCache() {
	srs.mutex.Lock()
	defer srs.mutex.Unlock()

	srs.cache = srs.cache[:0]
	srs.cache = append(srs.cache, []byte("# Runtime\n")...)

	srs.cache = utils.AppendInfoUint(srs.cache, "runtime_general_alloc:", srs.General.Alloc)
	srs.cache = utils.AppendInfoUint(srs.cache, "runtime_general_sys:", srs.General.Sys)
	srs.cache = utils.AppendInfoUint(srs.cache, "runtime_general_lookups:", srs.General.Lookups)
	srs.cache = utils.AppendInfoUint(srs.cache, "runtime_general_mallocs:", srs.General.Mallocs)
	srs.cache = utils.AppendInfoUint(srs.cache, "runtime_general_frees:", srs.General.Frees)

	srs.cache = utils.AppendInfoUint(srs.cache, "runtime_heap_alloc:", srs.Heap.Alloc)
	srs.cache = utils.AppendInfoUint(srs.cache, "runtime_heap_sys:", srs.Heap.Sys)
	srs.cache = utils.AppendInfoUint(srs.cache, "runtime_heap_idle:", srs.Heap.Idle)
	srs.cache = utils.AppendInfoUint(srs.cache, "runtime_heap_inuse:", srs.Heap.Inuse)
	srs.cache = utils.AppendInfoUint(srs.cache, "runtime_heap_objects:", srs.Heap.Objects)

	srs.cache = utils.AppendInfoUint(srs.cache, "runtime_gc_num:", uint64(srs.GC.Num))
	srs.cache = utils.AppendInfoFloat(srs.cache, "runtime_gc_cpu_fraction:", srs.GC.CPUFraction, 4)
	srs.cache = utils.AppendInfoUint(srs.cache, "runtime_gc_total_pausems:", srs.GC.TotalPauseMs)
	srs.cache = utils.AppendInfoUint(srs.cache, "runtime_num_procs:", uint64(srs.NumProcs))
	srs.cache = utils.AppendInfoUint(srs.cache, "runtime_num_goroutines:", uint64(srs.NumGoroutines))

	srs.cache = utils.AppendInfoFloat(srs.cache, "cpu:", srs.CPU, 4)
	srs.cache = utils.AppendInfoInt(srs.cache, "memory_total:", srs.MemoryTotal)
	srs.cache = utils.AppendInfoString(srs.cache, "memory_total_fmt:", butils.FmtSize(uint64(srs.MemoryTotal)))
	srs.cache = utils.AppendInfoInt(srs.cache, "memory_shr:", srs.MemoryShr)
	srs.cache = utils.AppendInfoString(srs.cache, "memory_shr_fmt:", butils.FmtSize(uint64(srs.MemoryShr)))

	srs.cache = append(srs.cache, '\n')
}

func (srs *SRuntimeStats) Samples() {
	sysUsage := utils.GetSysUsage()
	ms := sysUsage.MemStats
	srs.General.Alloc = ms.Alloc
	srs.General.Sys = ms.Sys
	srs.General.Lookups = ms.Lookups
	srs.General.Mallocs = ms.Mallocs
	srs.General.Frees = ms.Frees
	srs.Heap.Alloc = ms.HeapAlloc
	srs.Heap.Sys = ms.HeapSys
	srs.Heap.Idle = ms.HeapIdle
	srs.Heap.Inuse = ms.HeapInuse
	srs.Heap.Objects = ms.HeapObjects
	srs.GC.Num = ms.NumGC
	srs.GC.CPUFraction = ms.GCCPUFraction
	srs.GC.TotalPauseMs = ms.PauseTotalNs / uint64(time.Millisecond)
	srs.NumGoroutines = runtime.NumGoroutine()

	srs.MemoryShr = sysUsage.MemShr()
	srs.MemoryTotal = sysUsage.MemTotal() - srs.MemoryShr
	srs.CPU = sysUsage.CPU

	srs.UpdateCache()
}
