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

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/dbconfig"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/dbmeta"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

type Bitalos struct {
	Meta    *dbmeta.Meta
	Migrate *Migrate

	bitsdb *bitsdb.BitsDB
}

func NewBitalos(dir string) (*Bitalos, error) {
	cfg := newDbConfig(dir)
	dbPath := cfg.DBPath
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, err
	}

	meta, err := newBitalosMeta(dbPath)
	if err != nil {
		return nil, err
	}

	cfg.GetNextKeyId = meta.GetNextKeyUniqId
	cfg.GetCurrentKeyId = meta.GetCurrentKeyUniqId
	bdb, err := bitsdb.NewBitsDB(cfg, meta)
	if err != nil {
		return nil, err
	}

	b := &Bitalos{
		bitsdb: bdb,
		Meta:   meta,
	}

	b.tryClean()

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
	fmt.Fprintf(&buf, "EnableRaftlogRestore:%v ", cfg.EnableRaftlogRestore)
	fmt.Fprintf(&buf, "BithashCompressionType:%d ", cfg.BithashCompressionType)
	fmt.Fprintf(&buf, "WriteBufferSize:%d ", cfg.WriteBufferSize)
	fmt.Fprintf(&buf, "CompactStartTime:%d ", cfg.CompactStartTime)
	fmt.Fprintf(&buf, "CompactEndTime:%d ", cfg.CompactEndTime)
	fmt.Fprintf(&buf, "CompactInterval:%d ", cfg.CompactInterval)
	fmt.Fprintf(&buf, "BithashGcThreshold:%.3f ", cfg.BithashGcThreshold)
	fmt.Fprintf(&buf, "EnablePageBlockCompression:%v ", cfg.EnablePageBlockCompression)
	fmt.Fprintf(&buf, "PageBlockCacheSize:%v ", cfg.PageBlockCacheSize)
	fmt.Fprintf(&buf, "ExpiredDeletionQpsThreshold:%d ", config.GlobalConfig.Bitalos.ExpiredDeletionQpsThreshold)
	fmt.Fprintf(&buf, "EnableClockCache:%v ", config.GlobalConfig.Bitalos.EnableClockCache)
	fmt.Fprintf(&buf, "FlushPrefixDeleteKeyMultiplier:%d ", config.GlobalConfig.Bitalos.FlushPrefixDeleteKeyMultiplier)
	fmt.Fprintf(&buf, "FlushFileLifetime:%d ", config.GlobalConfig.Bitalos.FlushFileLifetime)
	fmt.Fprintf(&buf, "BitpageFlushSize:%d ", config.GlobalConfig.Bitalos.BitpageFlushSize)
	fmt.Fprintf(&buf, "BitpageSplitSize:%d ", config.GlobalConfig.Bitalos.BitpageSplitSize)

	fmt.Fprintf(&buf, "CacheSize:%d ", cfg.CacheSize)
	fmt.Fprintf(&buf, "CacheInitCap:%d ", cfg.CacheHashSize)
	fmt.Fprintf(&buf, "CacheShardNum:%d ", cfg.CacheShardNum)
	fmt.Fprintf(&buf, "CacheEliminateDuration:%d ", cfg.CacheEliminateDuration)
	fmt.Fprintf(&buf, "EnableMissCache:%v ", cfg.EnableMissCache)

	fmt.Fprintf(&buf, "MetaUpdateIndex:%d ", b.Meta.GetUpdateIndex())
	fmt.Fprintf(&buf, "MetaFlushIndex:%d ", b.Meta.GetFlushIndex())
	fmt.Fprintf(&buf, "MetaGetCurrentKeyUniqId:%d ", b.Meta.GetCurrentKeyUniqId())

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
	cfg.CompactInterval = config.GlobalConfig.Bitalos.CompactInterval
	cfg.BithashCompressionType = config.GlobalConfig.Bitalos.BithashCompressionType
	cfg.EnablePageBlockCompression = config.GlobalConfig.Bitalos.EnablePageBlockCompression
	cfg.PageBlockCacheSize = config.GlobalConfig.Bitalos.PageBlockCacheSize.AsInt()
	cfg.FlushPrefixDeleteKeyMultiplier = config.GlobalConfig.Bitalos.FlushPrefixDeleteKeyMultiplier
	cfg.FlushFileLifetime = config.GlobalConfig.Bitalos.FlushFileLifetime
	cfg.BitmapCacheItemCount = config.GlobalConfig.Bitalos.BitmapCacheItemCount
	cfg.BitpageFlushSize = config.GlobalConfig.Bitalos.BitpageFlushSize
	cfg.BitpageSplitSize = config.GlobalConfig.Bitalos.BitpageSplitSize
	if config.GlobalConfig.Bitalos.EnableWAL {
		cfg.DisableWAL = false
		cfg.EnableRaftlogRestore = false
	} else {
		cfg.DisableWAL = true
		cfg.EnableRaftlogRestore = config.GlobalConfig.Bitalos.EnableRaftlogRestore
	}
	return cfg
}

func newBitalosMeta(dir string) (*dbmeta.Meta, error) {
	meta, err := dbmeta.OpenMeta(dir)
	if err != nil {
		return nil, err
	}

	if !config.GlobalConfig.Bitalos.EnableRaftlogRestore {
		meta.SetFlushIndex(0)
	}

	cfgCompressionType := config.GlobalConfig.Bitalos.BithashCompressionType
	isSet, cType := meta.GetBitalosdbCompressTypeCfg()
	if isSet {
		config.GlobalConfig.Bitalos.BithashCompressionType = int(cType)
	} else {
		meta.SetBitalosdbCompressTypeCfg(uint16(cfgCompressionType))
	}

	return meta, nil
}

func (b *Bitalos) Close() {
	if b.bitsdb != nil {
		b.bitsdb.Close()
		b.bitsdb = nil
	}
	b.Meta.Close()
}

func (b *Bitalos) Flush(reason btools.FlushType, compactIndex uint64) {
	if reason == btools.FlushTypeCheckpoint {
		b.bitsdb.StringObj.BaseDb.BitmapMem.Flush(true)
	}
	b.bitsdb.Flush(reason, compactIndex)
}

func (b *Bitalos) IsOpenRaftRestore() bool {
	return b.bitsdb.IsOpenRaftRestore()
}

func (b *Bitalos) RaftReset() {
	b.Meta.RaftReset()
	b.bitsdb.RaftReset()
}

func (b *Bitalos) tryClean() {
	b.CleanSnapshot()
}

func (b *Bitalos) CleanSnapshot() {
	lastIndex := b.Meta.GetSnapshotIndex()
	if lastIndex <= 0 {
		return
	}

	snapshotPath := config.GetBitalosSnapshotPath()
	lastSnapshot := SnapshotDetail{SnapshotPath: snapshotPath, UpdateIndex: lastIndex}
	lastSnapshot.Clean()
}

func (b *Bitalos) BitalosdbUsage(bu *bitsdb.BitsUsage) {
	if b.bitsdb == nil {
		return
	}
	b.bitsdb.BitskvUsage(bu)
}

func (b *Bitalos) ScanDelExpire(jobId uint64) {
	if b.bitsdb == nil {
		return
	}

	b.bitsdb.ScanDeleteExpireDb(jobId)
}

func (b *Bitalos) ScanDelExpireAsync() {
	if b.bitsdb == nil {
		return
	}

	go func() {
		b.ScanDelExpire(0)
	}()
}

func (b *Bitalos) Compact() {
	if b.bitsdb == nil {
		return
	}

	go func() {
		b.bitsdb.Compact()
	}()
}

func (b *Bitalos) CompactExpire(start, end []byte) error {
	if b.bitsdb == nil {
		return nil
	}
	go func() {
		b.bitsdb.CompactExpire(start, end)
	}()
	return nil
}

func (b *Bitalos) CompactBitree() error {
	if b.bitsdb == nil {
		return nil
	}
	go func() {
		b.bitsdb.CompactBitree()
	}()
	return nil
}

func (b *Bitalos) DebugInfo() []byte {
	if b.bitsdb == nil {
		return nil
	}

	return b.bitsdb.DebugInfo()
}

func (b *Bitalos) CacheInfo() []byte {
	if b.bitsdb == nil {
		return nil
	}

	return b.bitsdb.CacheInfo()
}

func (b *Bitalos) GetIsDelExpire() int {
	if b.bitsdb == nil {
		return 0
	}
	return b.bitsdb.IsDelExpireRun()
}

func (b *Bitalos) IsBitsdbClosed() bool {
	return b.bitsdb == nil
}

func (b *Bitalos) CheckpointPrepareStart() {
	b.bitsdb.SetCheckpointHighPriority(true)
	b.bitsdb.CheckpointExpireLock(true)
	b.bitsdb.CheckpointPrepareForBitalosdb(true)
}

func (b *Bitalos) CheckpointPrepareEnd() {
	b.bitsdb.CheckpointPrepareForBitalosdb(false)
	b.bitsdb.CheckpointExpireLock(false)
	b.bitsdb.SetCheckpointHighPriority(false)
}

func (b *Bitalos) FlushAllDB() {
	b.bitsdb.FlushAllDB()
}

func (b *Bitalos) SetQPS(qps uint64) {
	if b.bitsdb != nil {
		b.bitsdb.SetQPS(qps)
	}
}

func (b *Bitalos) SetAutoCompact(val bool) {
	b.bitsdb.SetAutoCompact(val)
}
