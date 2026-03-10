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

package engine

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb"
	"github.com/zuoyebang/bitalostored/stored/engine/btools"
	"github.com/zuoyebang/bitalostored/stored/engine/dbconfig"
	"github.com/zuoyebang/bitalostored/stored/engine/dbmeta"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

type Bitalos struct {
	meta   *dbmeta.Meta
	bitsdb *bitsdb.BitsDB
}

func NewBitalos(dir string) (*Bitalos, error) {
	cfg := newDbConfig(dir)
	meta, err := NewMeta(cfg.DBPath)
	if err != nil {
		return nil, err
	}

	cfg.DisableStoreKey = config.GlobalConfig.Bitalos.DisableStoreKey
	cfg.CompressionType = config.GlobalConfig.Bitalos.CompressionType
	cfg.GetNextKeyId = meta.GetNextKeyUniqId
	cfg.GetCurrentKeyId = meta.GetCurrentKeyUniqId
	bdb, err := bitsdb.NewBitsDB(cfg)
	if err != nil {
		return nil, err
	}

	b := &Bitalos{
		bitsdb: bdb,
		meta:   meta,
	}

	b.tryClean()
	b.meta.Flush()

	log.Infof("new bitalos success dumpDbConfig[%s]", b.dumpDbConfig(cfg))
	return b, nil
}

func (b *Bitalos) dumpDbConfig(cfg *dbconfig.Config) string {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "DBPath:%s ", cfg.DBPath)
	fmt.Fprintf(&buf, "MaxFieldSize:%d ", btools.MaxFieldSize)
	fmt.Fprintf(&buf, "MaxValueSize:%d ", btools.MaxValueSize)
	fmt.Fprintf(&buf, "MaxIOWriteLoadQPS:%d ", btools.MaxIOWriteLoadQPS)
	fmt.Fprintf(&buf, "DisableWAL:%v ", cfg.DisableWAL)
	fmt.Fprintf(&buf, "CompressionType:%d ", cfg.CompressionType)
	fmt.Fprintf(&buf, "WriteBufferSize:%d ", cfg.WriteBufferSize)
	fmt.Fprintf(&buf, "CompactStartTime:%d ", cfg.CompactStartTime)
	fmt.Fprintf(&buf, "CompactEndTime:%d ", cfg.CompactEndTime)
	fmt.Fprintf(&buf, "BithashGcThreshold:%.3f ", cfg.BithashGcThreshold)
	fmt.Fprintf(&buf, "EnablePageBlockCompression:%v ", cfg.EnablePageBlockCompression)
	fmt.Fprintf(&buf, "PageBlockCacheSize:%v ", cfg.PageBlockCacheSize)
	fmt.Fprintf(&buf, "EnableClockCache:%v ", config.GlobalConfig.Bitalos.EnableClockCache)
	fmt.Fprintf(&buf, "FlushPrefixDeleteKeyMultiplier:%d ", config.GlobalConfig.Bitalos.FlushPrefixDeleteKeyMultiplier)
	fmt.Fprintf(&buf, "FlushFileLifetime:%d ", config.GlobalConfig.Bitalos.FlushFileLifetime)
	fmt.Fprintf(&buf, "VectorTableCount:%d ", cfg.VectorTableCount)
	fmt.Fprintf(&buf, "VectorTableHashSize:%d ", cfg.VectorTableHashSize)
	fmt.Fprintf(&buf, "VectorTableGcThreshold:%.3f ", cfg.VectorTableGcThreshold)
	fmt.Fprintf(&buf, "DisableStoreKey:%v ", cfg.DisableStoreKey)
	fmt.Fprintf(&buf, "BitpageFlushSize:%d ", cfg.BitpageFlushSize)
	fmt.Fprintf(&buf, "BitpageSplitSize:%d ", cfg.BitpageSplitSize)
	fmt.Fprintf(&buf, "BitpageTaskWorkerNum:%d ", cfg.BitpageTaskWorkerNum)
	fmt.Fprintf(&buf, "BitpageDisableMiniVi:%v ", cfg.BitpageDisableMiniVi)
	fmt.Fprintf(&buf, "MemTableSize:%d ", cfg.MemTableSize)
	fmt.Fprintf(&buf, "VmTableSize:%d ", cfg.VmTableSize)

	fmt.Fprintf(&buf, "CacheSize:%d ", cfg.CacheSize)
	fmt.Fprintf(&buf, "CacheInitCap:%d ", cfg.CacheHashSize)
	fmt.Fprintf(&buf, "CacheShardNum:%d ", cfg.CacheShardNum)
	fmt.Fprintf(&buf, "CacheEliminateDuration:%d ", cfg.CacheEliminateDuration)
	fmt.Fprintf(&buf, "EnableMissCache:%v ", cfg.EnableMissCache)

	fmt.Fprintf(&buf, "MetaUpdateIndex:%d ", b.meta.GetUpdateIndex())
	fmt.Fprintf(&buf, "MetaFlushIndex:%d ", b.meta.GetFlushIndex())
	fmt.Fprintf(&buf, "MetaGetCurrentKeyUniqId:%d ", b.meta.GetCurrentKeyUniqId())

	return buf.String()
}

func newDbConfig(path string) *dbconfig.Config {
	cfg := dbconfig.NewConfigDefault()
	cfg.DBPath = path
	cfg.WriteBufferSize = config.GlobalConfig.Bitalos.WriteBufferSize.AsInt()
	cfg.CacheSize = config.GlobalConfig.Bitalos.CacheSize.AsInt()
	cfg.CacheHashSize = config.GlobalConfig.Bitalos.CacheHashSize
	cfg.CacheShardNum = config.GlobalConfig.Bitalos.CacheShardNum
	cfg.CacheEliminateDuration = config.GlobalConfig.Bitalos.CacheEliminateDuration
	cfg.EnableMissCache = config.GlobalConfig.Bitalos.EnableMissCache
	cfg.CompactStartTime = config.GlobalConfig.Bitalos.CompactStartTime
	cfg.CompactEndTime = config.GlobalConfig.Bitalos.CompactEndTime
	cfg.BithashGcThreshold = config.GlobalConfig.Bitalos.BithashGcThreshold
	cfg.BithashCompressionType = config.GlobalConfig.Bitalos.BithashCompressionType
	cfg.CompressionType = config.GlobalConfig.Bitalos.CompressionType
	cfg.EnablePageBlockCompression = config.GlobalConfig.Bitalos.EnablePageBlockCompression
	cfg.PageBlockCacheSize = config.GlobalConfig.Bitalos.PageBlockCacheSize.AsInt()
	cfg.FlushPrefixDeleteKeyMultiplier = config.GlobalConfig.Bitalos.FlushPrefixDeleteKeyMultiplier
	cfg.FlushFileLifetime = config.GlobalConfig.Bitalos.FlushFileLifetime
	cfg.VectorTableCount = config.GlobalConfig.Bitalos.VectorTableCount
	cfg.VectorTableHashSize = config.GlobalConfig.Bitalos.VectorTableHashSize
	cfg.VectorTableGcThreshold = config.GlobalConfig.Bitalos.VectorTableGcThreshold
	cfg.DisableStoreKey = config.GlobalConfig.Bitalos.DisableStoreKey
	cfg.BitmapCacheItemCount = config.GlobalConfig.Bitalos.BitmapCacheItemCount
	cfg.BitpageFlushSize = config.GlobalConfig.Bitalos.BitpageFlushSize.AsInt()
	cfg.BitpageSplitSize = config.GlobalConfig.Bitalos.BitpageSplitSize.AsInt()
	cfg.BitpageTaskWorkerNum = config.GlobalConfig.Bitalos.BitpageTaskWorkerNum
	cfg.BitpageDisableMiniVi = config.GlobalConfig.Bitalos.BitpageDisableMiniVi
	cfg.MemTableSize = config.GlobalConfig.Bitalos.MemTableSize.AsInt()
	cfg.VmTableSize = config.GlobalConfig.Bitalos.VmTableSize.AsInt()
	return cfg
}

func NewMeta(dir string) (*dbmeta.Meta, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	meta, err := dbmeta.OpenMeta(dir)
	if err != nil {
		return nil, err
	}

	meta.SetFlushIndex(0)

	if !meta.IsMigrateV6Completed() {
		_, ocType := meta.GetBitalosdbCompressTypeCfg()
		if config.GlobalConfig.Bitalos.CompressionType != int(ocType) {
			meta.SetBitalosdbCompressTypeCfg(uint16(config.GlobalConfig.Bitalos.CompressionType))
			log.Infof("migrateV6 meta reset CompressionType set %d to %d", ocType, config.GlobalConfig.Bitalos.CompressionType)
		}
	}

	isSetCompressionType, cType := meta.GetBitalosdbCompressTypeCfg()
	if isSetCompressionType {
		config.GlobalConfig.Bitalos.CompressionType = int(cType)
	} else {
		meta.SetBitalosdbCompressTypeCfg(uint16(config.GlobalConfig.Bitalos.CompressionType))
	}

	isSetStoreKey, disableStoreKey := meta.GetBitalosdbStoreKeyCfg()
	if isSetStoreKey {
		config.GlobalConfig.Bitalos.DisableStoreKey = disableStoreKey
	} else {
		if config.GlobalConfig.Bitalos.DisableStoreKey {
			meta.SetBitalosdbStoreKeyCfg(1)
		} else {
			meta.SetBitalosdbStoreKeyCfg(2)
		}
	}

	log.Infof("meta load config isSetCompressionType:%v CompressionType:%d isSetStoreKey:%v DisableStoreKey:%v",
		isSetCompressionType, cType, isSetStoreKey, disableStoreKey)

	return meta, nil
}

func (b *Bitalos) Close() {
	b.bitsdb.Close()
	b.meta.Close()
}

func (b *Bitalos) RaftReset() {
	b.meta.RaftReset()
}

func (b *Bitalos) tryClean() {
	snapshotPath := config.GetBitalosSnapshotPath()
	if err := os.RemoveAll(snapshotPath); err != nil {
		log.Infof("remove snapshot err:%s", err)
	} else {
		log.Info("remove snapshot succ")
	}
}

func (b *Bitalos) CleanSnapshot(clusterId uint64) {
	snapshotPath := config.GetBitalosSnapshotPath()
	lastSnapshot := SnapshotDetail{SnapshotPath: snapshotPath, IsRoot: true, ClusterId: uint64(clusterId)}
	lastSnapshot.Clean()
}

func (b *Bitalos) BitalosdbUsage(bu *bitsdb.BitsUsage) {
	if b.bitsdb == nil {
		return
	}
	b.bitsdb.BitskvUsage(bu)
}

func (b *Bitalos) Compact() {
	if b.bitsdb == nil {
		return
	}

	go func() {
		b.bitsdb.Compact()
	}()
}

func (b *Bitalos) VtableGC(slotId uint16) {
	if b.bitsdb == nil {
		return
	}

	go func() {
		b.bitsdb.DB.VtableGC(slotId)
	}()
}

func (b *Bitalos) FlushBitpage() {
	if b.bitsdb == nil {
		return
	}

	go func() {
		b.bitsdb.DB.FlushBitpage()
	}()
}

func (b *Bitalos) FlushDB(flushForce bool) {
	if b.bitsdb == nil {
		return
	}

	go func() {
		b.bitsdb.DB.Flush(flushForce)
	}()
}

func (b *Bitalos) DebugInfo() []byte {
	if b.bitsdb == nil {
		return nil
	}

	return b.bitsdb.DebugInfo()
}

func (b *Bitalos) RemoveSlot(slot uint16) error {
	if b.bitsdb == nil {
		return nil
	}

	return b.bitsdb.RemoveSlot(slot)
}

func (b *Bitalos) DirDiskInfo() []byte {
	if b.bitsdb == nil {
		return nil
	}

	return b.bitsdb.DirDiskInfo()
}

func (b *Bitalos) CacheInfo() []byte {
	if b.bitsdb == nil {
		return nil
	}

	cacheInfo := b.bitsdb.CacheInfo()
	return []byte(cacheInfo)
}

func (b *Bitalos) IsBitsdbClosed() bool {
	return b.bitsdb == nil
}

func (b *Bitalos) SetQPS(qps uint64) {
	if b.bitsdb != nil {
		b.bitsdb.SetQPS(qps)
	}
}

func (b *Bitalos) SetAutoCompact(val bool) {
	b.bitsdb.DB.SetAutoCompact(val)
}

func (b *Bitalos) Checkpoint(path string) (func(), error) {
	if err := b.meta.Checkpoint(path); err != nil {
		return nil, err
	}

	b.bitsdb.BitmapMem.Flush(true)

	dbPath := filepath.Join(path, bitsdb.DataDbDirname)
	return b.bitsdb.DB.Checkpoint(dbPath)
}

func (b *Bitalos) FlushMem() {
	b.bitsdb.BitmapMem.Flush(true)
}

func (b *Bitalos) GetMeta() *dbmeta.Meta {
	return b.meta
}

func (b *Bitalos) GetCurrentKeyUniqId() uint64 {
	return b.meta.GetCurrentKeyUniqId()
}

func (b *Bitalos) GetUpdateIndex() uint64 {
	return b.meta.GetUpdateIndex()
}

func (b *Bitalos) SetUpdateIndex(index uint64) {
	b.meta.SetUpdateIndex(index)
}

func (b *Bitalos) SetSnapshotIndex(index uint64) uint64 {
	return b.meta.SetSnapshotIndex(index)
}
