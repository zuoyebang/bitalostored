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
	"bytes"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalosdb/v2"
	"github.com/zuoyebang/bitalostored/butils"
	"github.com/zuoyebang/bitalostored/butils/vectormap"
	"github.com/zuoyebang/bitalostored/stored/engine/btools"
	"github.com/zuoyebang/bitalostored/stored/engine/dbconfig"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/locker"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

type BitsDB struct {
	DB          *bitalosdb.DB
	BitmapMem   *BitmapMem
	StringCache *vectormap.VectorMap

	debugInfo       DBDebugInfo
	diskInfo        DBDiskInfo
	diskDetail      DBDiskDetail
	dbStats         DBStats
	keyLocker       *locker.ScopeLocker
	ready           atomic.Bool
	enableMissCache bool
	statQPS         atomic.Uint64
	statMemShr      atomic.Uint64
}

func NewBitsDB(cfg *dbconfig.Config) (*BitsDB, error) {
	btools.SetDefineVarFromCfg()

	bdb := &BitsDB{
		StringCache:     nil,
		enableMissCache: false,
	}
	cfg.IOWriteLoadThresholdFunc = bdb.CheckIOWriteLoadThreshold
	dbPath := filepath.Join(cfg.DBPath, DataDbDirname)
	db, err := openBitvectorDB(dbPath, cfg)
	if err != nil {
		return nil, err
	}

	bdb.DB = db
	bdb.BitmapMem = NewBitmapMem(bdb, cfg.BitmapCacheItemCount)
	bdb.keyLocker = locker.NewScopeLocker(true)
	bdb.debugInfo = DBDebugInfo{}
	bdb.diskInfo = DBDiskInfo{}
	bdb.diskDetail = DBDiskDetail{}
	bdb.dbStats = DBStats{}

	if cfg.CacheSize > 0 {
		if cfg.CacheEliminateDuration <= 0 {
			cfg.CacheEliminateDuration = defaultCacheEliminateDuration
		}
		if cfg.CacheShardNum < defaultCacheShardNum {
			cfg.CacheShardNum = defaultCacheShardNum
		}

		bdb.enableMissCache = cfg.EnableMissCache
		bdb.StringCache = vectormap.NewVectorMap(uint32(cfg.CacheHashSize),
			vectormap.WithType(vectormap.MapTypeLRU),
			vectormap.WithBuckets(cfg.CacheShardNum),
			vectormap.WithLogger(log.GetLogger()),
			vectormap.WithEliminate(vectormap.Byte(cfg.CacheSize), defaultCacheEliminateThreadNum, time.Duration(cfg.CacheEliminateDuration)*time.Second))
	}

	bdb.ready.Store(true)
	return bdb, nil
}

func openBitvectorDB(dirname string, cfg *dbconfig.Config) (*bitalosdb.DB, error) {
	compactOpt := bitalosdb.CompactEnv{
		StartHour:            cfg.CompactStartTime,
		EndHour:              cfg.CompactEndTime,
		BithashDeletePercent: cfg.BithashGcThreshold,
		VtGCThreshold:        cfg.VectorTableGcThreshold,
	}

	opts := &bitalosdb.Options{
		CompressionType:                cfg.CompressionType,
		Logger:                         log.GetLogger(),
		Verbose:                        true,
		LogTag:                         "[bitalosdb]",
		MaxKeySize:                     btools.MaxKeySize,
		MaxSubKeySize:                  btools.MaxFieldSize,
		MaxValueSize:                   btools.MaxValueSize,
		BytesPerSync:                   1 << 20,
		DeleteFileInternal:             10,
		AutoCompact:                    true,
		CompactInfo:                    compactOpt,
		BitpageFlushSize:               uint64(cfg.BitpageFlushSize),
		BitpageSplitSize:               uint64(cfg.BitpageSplitSize),
		BitpageTaskWorkerNum:           cfg.BitpageTaskWorkerNum,
		BitpageDisableMiniVi:           cfg.BitpageDisableMiniVi,
		DisableStoreKey:                cfg.DisableStoreKey,
		VmTableSize:                    cfg.VmTableSize,
		MemTableSize:                   cfg.MemTableSize,
		VectorTableCount:               uint16(cfg.VectorTableCount),
		VectorTableHashSize:            uint32(cfg.VectorTableHashSize),
		IOWriteLoadThresholdFunc:       cfg.IOWriteLoadThresholdFunc,
		FlushPrefixDeleteKeyMultiplier: cfg.FlushPrefixDeleteKeyMultiplier,
		FlushFileLifetime:              cfg.FlushFileLifetime,
	}

	opts.GetNowTimestamp = func() uint64 {
		return uint64(tclock.GetTimestampMilli())
	}

	db, err := bitalosdb.Open(dirname, opts)
	if err != nil {
		return nil, err
	}

	log.Infof("open bitvector success dirname:%s memSize:%d, vmSize:%d", dirname, opts.MemTableSize, opts.VmTableSize)
	return db, nil
}

func (bdb *BitsDB) IsReady() bool {
	return bdb.ready.Load()
}

func (bdb *BitsDB) ClearCache() {
	if bdb.StringCache != nil {
		bdb.StringCache.Clear()
	}
}

func (bdb *BitsDB) Close() {
	log.Infof("bitsDB Close start")

	bdb.BitmapMem.Close()
	if err := bdb.DB.Close(); err != nil {
		log.Infof("bitsDB db close err:%s", err)
	}
	bdb.ClearCache()

	log.Infof("bitsDB Close finish")
}

func (bdb *BitsDB) Compact() {
}

func (bdb *BitsDB) RemoveSlot(slot uint16) error {
	return bdb.DB.RemoveSlot(slot)
}

func (bdb *BitsDB) DebugInfo() []byte {
	var buf bytes.Buffer

	bdb.debugInfo.PBDbInfo = bdb.DB.DebugInfo()
	buf.Write(bdb.debugInfo.Marshal())

	return buf.Bytes()
}

func (bdb *BitsDB) DiskDetail() DBDiskDetail {
	bdb.diskDetail.BitupleDisk, bdb.diskDetail.BitpageDisk, bdb.diskDetail.BithashDisk = bdb.DB.DiskInfo()
	return bdb.diskDetail
}

func (bdb *BitsDB) GetDbStats() DBStats {
	dbStats := bdb.DB.GetStats()
	bdb.dbStats.VmTableFlushLastCost = dbStats.VmTableFlushLastCost
	bdb.dbStats.VmTableFlushAvgCost = dbStats.VmTableFlushAvgCost
	bdb.dbStats.MemTableFlushLastCost = dbStats.MemTableFlushLastCost
	bdb.dbStats.MemTableFlushAvgCost = dbStats.MemTableFlushAvgCost
	return bdb.dbStats
}

func (bdb *BitsDB) DirDiskInfo() []byte {
	var buf bytes.Buffer

	bdb.diskInfo.PBDbInfo = bdb.DB.DirDiskInfo()
	buf.Write(bdb.diskInfo.Marshal())

	return buf.Bytes()
}

func (bdb *BitsDB) CacheInfo() string {
	if bdb.StringCache == nil {
		return ""
	}

	cache := bdb.StringCache

	memCap := cache.MaxMem()
	usedMem := cache.UsedMem()
	sahrdNum := cache.Shards()
	effectiveMem := cache.EffectiveMem()
	remainItemNum := cache.Capacity()
	itemNum := cache.Count()
	reputFailsCount := cache.RePutFails()
	missCount := cache.MissCount()
	queryCount := cache.QueryCount()
	var hitRate float64
	if queryCount > 0 {
		hitRate = float64(queryCount-missCount) / float64(queryCount)
	}

	return fmt.Sprintf("shardNum:%d memCap:%d usedMem:%d effectiveMem:%d remainItem:%d Items:%d reputFailsCount:%d queryCount:%d missCount:%d hitRate:%.6f",
		sahrdNum, memCap, usedMem, effectiveMem, remainItemNum, itemNum, reputFailsCount, queryCount, missCount, hitRate)
}

func (bdb *BitsDB) BitskvUsage(bu *BitsUsage) {
	if bdb == nil || !bdb.IsReady() {
		return
	}

	bu.dbUsage.SetDataDiskSize(butils.GetDirSize(config.GetBitalosDataDbPath()))
	bu.dbUsage.SetDiskDetail(bdb.DiskDetail())
	bu.dbUsage.SetFlushUsage(bdb.GetDbStats())

	bu.UpdateCache()
}

func (bdb *BitsDB) SetQPS(v uint64) {
	bdb.statQPS.Store(v)
}

func (bdb *BitsDB) CheckIOWriteLoadThreshold() bool {
	if btools.MaxIOWriteLoadQPS == 0 {
		return true
	}
	qps := bdb.statQPS.Load()
	return qps < btools.MaxIOWriteLoadQPS
}

func (bdb *BitsDB) GetMemoryShr() int64 {
	sysUsage := utils.GetSysUsage()
	return sysUsage.MemShr()
}

func (bdb *BitsDB) LockKey(khash uint32) func() {
	return bdb.keyLocker.LockWriteKey(khash)
}
