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

package bitsdb

import (
	"sync"

	"github.com/zuoyebang/bitalostored/stored/internal/bytepools"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"

	"github.com/zuoyebang/bitalostored/butils"
)

type Usage struct {
	DataDiskSize          int64  `json:"data_disk_size"`
	BitpageDiskSize       uint64 `json:"bitpage_disk_size"`
	BithashDiskSize       uint64 `json:"bithash_disk_size"`
	BitupleDiskSize       uint64 `json:"bituple_disk_size"`
	VmTableFlushLastCost  int64  `json:"vmtable_flush_last_cost"`
	VmTableFlushAvgCost   int64  `json:"vmtable_flush_avg_cost"`
	MemTableFlushLastCost int64  `json:"memtable_flush_last_cost"`
	MemTableFlushAvgCost  int64  `json:"memtable_flush_avg_cost"`
}

func (u *Usage) SetDataDiskSize(size int64) {
	u.DataDiskSize = size
}

func (u *Usage) SetDiskDetail(d DBDiskDetail) {
	u.BitpageDiskSize = d.BitpageDisk
	u.BithashDiskSize = d.BithashDisk
	u.BitupleDiskSize = d.BitupleDisk
}

func (u *Usage) SetFlushUsage(s DBStats) {
	u.VmTableFlushLastCost = s.VmTableFlushLastCost
	u.VmTableFlushAvgCost = s.VmTableFlushAvgCost
	u.MemTableFlushLastCost = s.MemTableFlushLastCost
	u.MemTableFlushAvgCost = s.MemTableFlushAvgCost
}

type BitsUsage struct {
	dbUsage *Usage
	mutex   sync.RWMutex
	cache   []byte
}

func NewBitsUsage() *BitsUsage {
	return &BitsUsage{
		dbUsage: &Usage{},
		cache:   make([]byte, 0, 6144),
	}
}

func (bu *BitsUsage) Marshal() ([]byte, func()) {
	bu.mutex.RLock()
	defer bu.mutex.RUnlock()

	info, closer := bytepools.GlobalBytePools.GetBytePool(len(bu.cache))
	num := copy(info[0:], bu.cache)
	return info[:num], closer
}

func (bu *BitsUsage) AppendTo(target []byte, pos int) int {
	bu.mutex.RLock()
	defer bu.mutex.RUnlock()

	return copy(target[pos:], bu.cache)
}

func (bu *BitsUsage) UpdateCache() {
	bu.mutex.Lock()
	defer bu.mutex.Unlock()

	AppendInfoInt := utils.AppendInfoInt
	AppendInfoString := utils.AppendInfoString

	bu.cache = bu.cache[:0]
	bu.cache = append(bu.cache, []byte("# Bitalosdb\n")...)

	bu.cache = AppendInfoInt(bu.cache, "data_disk_size:", bu.dbUsage.DataDiskSize)
	bu.cache = AppendInfoString(bu.cache, "data_disk_fmt_size:", butils.FmtSize(uint64(bu.dbUsage.DataDiskSize)))
	bu.cache = AppendInfoInt(bu.cache, "bituple_disk_size:", int64(bu.dbUsage.BitupleDiskSize))
	bu.cache = AppendInfoString(bu.cache, "bituple_disk_fmt_size:", butils.FmtSize(bu.dbUsage.BitupleDiskSize))
	bu.cache = AppendInfoInt(bu.cache, "bitpage_disk_size:", int64(bu.dbUsage.BitpageDiskSize))
	bu.cache = AppendInfoString(bu.cache, "bitpage_disk_fmt_size:", butils.FmtSize(bu.dbUsage.BitpageDiskSize))
	bu.cache = AppendInfoInt(bu.cache, "bithash_disk_size:", int64(bu.dbUsage.BithashDiskSize))
	bu.cache = AppendInfoString(bu.cache, "bithash_disk_fmt_size:", butils.FmtSize(bu.dbUsage.BithashDiskSize))

	bu.cache = AppendInfoInt(bu.cache, "vmtable_flush_last_cost:", bu.dbUsage.VmTableFlushLastCost)
	bu.cache = AppendInfoInt(bu.cache, "vmtable_flush_avg_cost:", bu.dbUsage.VmTableFlushAvgCost)
	bu.cache = AppendInfoInt(bu.cache, "memtable_flush_last_cost:", bu.dbUsage.MemTableFlushLastCost)
	bu.cache = AppendInfoInt(bu.cache, "memtable_flush_avg_cost:", bu.dbUsage.MemTableFlushAvgCost)

	bu.cache = append(bu.cache, '\n')
}
