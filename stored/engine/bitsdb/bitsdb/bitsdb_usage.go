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

	"github.com/zuoyebang/bitalostored/butils"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/internal/bytepools"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

type BitsDBUsage struct {
	DataDiskSize           int64 `json:"data_disk_size"`
	DataFlushMemTime       int64 `json:"data_flush_mem_time"`
	DataBithashFileTotal   int   `json:"data_bithash_file_total"`
	DataBithashKeyTotal    int   `json:"data_bithash_key_total"`
	DataBithashDelKeyTotal int   `json:"data_bithash_del_key_total"`
	IndexDiskSize          int64 `json:"index_disk_size"`
	IndexFlushMemTime      int64 `json:"index_flush_mem_time"`
	MetaDiskSize           int64 `json:"meta_disk_size"`
	MetaFlushMemTime       int64 `json:"meta_flush_mem_time"`
	ExpireDiskSize         int64 `json:"expire_disk_size"`
}

func (u *BitsDBUsage) SetDataStats(stat bitskv.MetricsInfo) {
	u.DataFlushMemTime = stat.FlushMemTime
	u.DataBithashFileTotal = stat.BithashFileTotal
	u.DataBithashKeyTotal = stat.BithashKeyTotal
	u.DataBithashDelKeyTotal = stat.BithashDelKeyTotal
}

func (u *BitsDBUsage) SetMetaStats(stat bitskv.MetricsInfo) {
	u.MetaFlushMemTime = stat.FlushMemTime
}

func (u *BitsDBUsage) SetIndexStats(stat bitskv.MetricsInfo) {
	u.IndexFlushMemTime = stat.FlushMemTime
}

func (u *BitsDBUsage) SetExpireDiskSize(size int64) {
	u.ExpireDiskSize = size
}

func (u *BitsDBUsage) SetDataDiskSize(size int64) {
	u.DataDiskSize = size
}

func (u *BitsDBUsage) SetMetaDiskSize(size int64) {
	u.MetaDiskSize = size
}

func (u *BitsDBUsage) SetIndexDiskSize(size int64) {
	u.IndexDiskSize = size
}

type BitsUsage struct {
	metaUsage *BitsDBUsage
	hashUsage *BitsDBUsage
	listUsage *BitsDBUsage
	zsetUsage *BitsDBUsage
	setUsage  *BitsDBUsage

	mutex sync.RWMutex
	cache []byte
}

func NewBitsUsage() *BitsUsage {
	return &BitsUsage{
		metaUsage: &BitsDBUsage{},
		hashUsage: &BitsDBUsage{},
		listUsage: &BitsDBUsage{},
		zsetUsage: &BitsDBUsage{},
		setUsage:  &BitsDBUsage{},
		cache:     make([]byte, 0, 6144),
	}
}

func (bu *BitsUsage) Marshal() ([]byte, func()) {
	bu.mutex.RLock()
	defer bu.mutex.RUnlock()

	info, closer := bytepools.BytePools.GetBytePool(len(bu.cache))
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

	bu.cache = AppendInfoInt(bu.cache, "string_data_disk_size:", bu.metaUsage.DataDiskSize)
	bu.cache = AppendInfoString(bu.cache, "string_data_disk_fmt_size:", butils.FmtSize(uint64(bu.metaUsage.DataDiskSize)))
	bu.cache = AppendInfoInt(bu.cache, "string_data_flush_mem_time:", bu.metaUsage.DataFlushMemTime)
	bu.cache = AppendInfoInt(bu.cache, "string_data_bithash_file:", int64(bu.metaUsage.DataBithashFileTotal))
	bu.cache = AppendInfoInt(bu.cache, "string_data_bithash_add_key:", int64(bu.metaUsage.DataBithashKeyTotal))
	bu.cache = AppendInfoInt(bu.cache, "string_data_bithash_delete_key:", int64(bu.metaUsage.DataBithashDelKeyTotal))
	bu.cache = AppendInfoInt(bu.cache, "string_expire_disk_size:", bu.metaUsage.ExpireDiskSize)
	bu.cache = AppendInfoString(bu.cache, "string_expire_disk_fmt_size:", butils.FmtSize(uint64(bu.metaUsage.ExpireDiskSize)))

	bu.cache = AppendInfoInt(bu.cache, "hash_data_disk_size:", bu.hashUsage.DataDiskSize)
	bu.cache = AppendInfoString(bu.cache, "hash_data_disk_fmt_size:", butils.FmtSize(uint64(bu.hashUsage.DataDiskSize)))
	bu.cache = AppendInfoInt(bu.cache, "hash_data_flush_mem_time:", bu.hashUsage.DataFlushMemTime)
	bu.cache = AppendInfoInt(bu.cache, "hash_data_bithash_file:", int64(bu.hashUsage.DataBithashFileTotal))
	bu.cache = AppendInfoInt(bu.cache, "hash_data_bithash_add_key:", int64(bu.hashUsage.DataBithashKeyTotal))
	bu.cache = AppendInfoInt(bu.cache, "hash_data_bithash_delete_key:", int64(bu.hashUsage.DataBithashDelKeyTotal))

	bu.cache = AppendInfoInt(bu.cache, "list_data_disk_size:", bu.listUsage.DataDiskSize)
	bu.cache = AppendInfoString(bu.cache, "list_data_disk_fmt_size:", butils.FmtSize(uint64(bu.listUsage.DataDiskSize)))
	bu.cache = AppendInfoInt(bu.cache, "list_data_flush_mem_time:", bu.listUsage.DataFlushMemTime)
	bu.cache = AppendInfoInt(bu.cache, "list_data_bithash_file:", int64(bu.listUsage.DataBithashFileTotal))
	bu.cache = AppendInfoInt(bu.cache, "list_data_bithash_add_key:", int64(bu.listUsage.DataBithashKeyTotal))
	bu.cache = AppendInfoInt(bu.cache, "list_data_bithash_delete_key:", int64(bu.listUsage.DataBithashDelKeyTotal))

	bu.cache = AppendInfoInt(bu.cache, "set_data_disk_size:", bu.setUsage.DataDiskSize)
	bu.cache = AppendInfoString(bu.cache, "set_data_disk_fmt_size:", butils.FmtSize(uint64(bu.setUsage.DataDiskSize)))
	bu.cache = AppendInfoInt(bu.cache, "set_data_flush_mem_time:", bu.setUsage.DataFlushMemTime)

	bu.cache = AppendInfoInt(bu.cache, "zset_data_disk_size:", bu.zsetUsage.DataDiskSize)
	bu.cache = AppendInfoString(bu.cache, "zset_data_disk_fmt_size:", butils.FmtSize(uint64(bu.zsetUsage.DataDiskSize)))
	bu.cache = AppendInfoInt(bu.cache, "zset_data_flush_mem_time:", bu.zsetUsage.DataFlushMemTime)
	bu.cache = AppendInfoInt(bu.cache, "zset_index_disk_size:", bu.zsetUsage.IndexDiskSize)
	bu.cache = AppendInfoString(bu.cache, "zset_index_disk_fmt_size:", butils.FmtSize(uint64(bu.zsetUsage.IndexDiskSize)))
	bu.cache = AppendInfoInt(bu.cache, "zset_index_flush_mem_time:", bu.zsetUsage.IndexFlushMemTime)

	bu.cache = append(bu.cache, '\n')
}
