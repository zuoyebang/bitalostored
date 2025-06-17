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

package base

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalostored/butils/vectormap"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/locker"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv/kv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/dbconfig"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

const (
	defaultCacheShardNum           int = 1024
	defaultCacheEliminateThreadNum int = 1
	defaultCacheEliminateDuration  int = 1080

	missCacheValue = byte(btools.NoneType)
)

type BaseDB struct {
	DB              *bitskv.DB
	MetaCache       *vectormap.VectorMap
	EnableMissCache bool
	IsKeyScan       atomic.Int32
	Ready           atomic.Bool
	KeyLocker       *locker.ScopeLocker
	BitmapMem       *BitmapMem
}

func NewBaseDB(cfg *dbconfig.Config) (*BaseDB, error) {
	db, err := bitskv.NewBaseDB(cfg)
	if err != nil {
		return nil, err
	}

	baseDb := &BaseDB{
		DB:              db,
		KeyLocker:       locker.NewScopeLocker(true),
		MetaCache:       nil,
		EnableMissCache: false,
	}
	baseDb.BitmapMem = NewBitmapMem(baseDb, cfg.BitmapCacheItemCount)

	if cfg.CacheSize > 0 {
		if cfg.CacheEliminateDuration <= 0 {
			cfg.CacheEliminateDuration = defaultCacheEliminateDuration
		}
		if cfg.CacheShardNum < defaultCacheShardNum {
			cfg.CacheShardNum = defaultCacheShardNum
		}

		baseDb.EnableMissCache = cfg.EnableMissCache
		baseDb.MetaCache = vectormap.NewVectorMap(uint32(cfg.CacheHashSize),
			vectormap.WithType(vectormap.MapTypeLRU),
			vectormap.WithBuckets(cfg.CacheShardNum),
			vectormap.WithLogger(log.GetLogger()),
			vectormap.WithEliminate(vectormap.Byte(cfg.CacheSize), defaultCacheEliminateThreadNum, time.Duration(cfg.CacheEliminateDuration)*time.Second))
	}

	return baseDb, nil
}

func (b *BaseDB) SetReady() {
	b.Ready.Store(true)
}

func (b *BaseDB) SetNoReady() {
	b.Ready.Store(false)
}

func (b *BaseDB) IsReady() bool {
	return b.Ready.Load()
}

func (b *BaseDB) Close() {
	b.SetNoReady()
	b.DB.Close()
	if b.MetaCache != nil {
		b.MetaCache.Close()
		log.Infof("MetaCache Close finish")
	}
}

func (b *BaseDB) FlushBitmap() {
	b.BitmapMem.Close()
}

func (b *BaseDB) ClearCache() {
	if b.MetaCache != nil {
		b.MetaCache.Clear()
	}
}

func (b *BaseDB) CompactExpire(start, end []byte) error {
	return b.DB.CompactExpire(start, end)
}

func (b *BaseDB) GetMeta(key []byte) ([]byte, func(), error) {
	if b.MetaCache != nil {
		v, closer, exist := b.MetaCache.Get(key)
		if exist {
			if b.EnableMissCache && v != nil && v[0] == missCacheValue {
				closer()
				return nil, nil, nil
			}
			return v, closer, nil
		}
	}

	val, closer, err := b.DB.GetMeta(key)
	if b.DB.IsNotFound(err) {
		if b.EnableMissCache {
			b.MetaCache.RePut(key, []byte{missCacheValue})
		}
		return nil, nil, nil
	}

	if b.MetaCache != nil && len(val) > 0 {
		b.MetaCache.RePut(key, val)
	}

	return val, closer, err
}

func (b *BaseDB) BaseGetMetaWithoutValue(ek []byte) (*MetaData, error) {
	return b.getMetaWithoutValue(ek, btools.NoneType)
}

func (b *BaseDB) BaseGetMetaWithValue(ek []byte) (*MetaData, func(), error) {
	return b.getMetaWithValue(ek, btools.NoneType)
}

func (b *BaseDB) BaseGetMetaDataCheckAlive(key []byte, khash uint32) (*MetaData, error) {
	mk, mkCloser := EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := b.BaseGetMetaWithoutValue(mk)
	if mkv == nil || err != nil {
		return nil, err
	}

	if !mkv.IsAlive() {
		PutMkvToPool(mkv)
		return nil, nil
	}

	return mkv, nil
}

func (b *BaseDB) getMetaWithValue(ek []byte, dt btools.DataType) (mkv *MetaData, _ func(), _ error) {
	v, vcloser, err := b.GetMeta(ek)
	defer func() {
		if mkv == nil && vcloser != nil {
			vcloser()
		}
	}()
	if err != nil || len(v) <= 0 {
		return nil, nil, err
	}

	mkv = GetMkvFromPool()
	if err = DecodeMetaValue(mkv, v); err != nil {
		PutMkvToPool(mkv)
		return nil, nil, err
	}

	if mkv.IsWrongType(dt) {
		log.Errorf("getMetaWithValue dataType notmatch ek:%s exp:%d act:%d mkv:%v", string(ek), dt, mkv.dt, mkv)
		PutMkvToPool(mkv)
		return nil, nil, errn.ErrWrongType
	}

	return mkv, vcloser, nil
}

func (b *BaseDB) getMetaWithoutValue(ek []byte, dt btools.DataType) (*MetaData, error) {
	mkv, vcloser, err := b.getMetaWithValue(ek, dt)
	defer func() {
		if vcloser != nil {
			vcloser()
		}
	}()
	if err != nil {
		return nil, err
	}

	if mkv == nil {
		mkv = GetMkvFromPool()
		mkv.dt = dt
	}

	return mkv, nil
}

func (b *BaseDB) DeleteMetaKey(key []byte) error {
	wb := b.DB.GetMetaWriteBatchFromPool()
	defer b.DB.PutWriteBatchToPool(wb)

	_ = wb.Delete(key)
	err := wb.Commit()
	if err == nil && b.MetaCache != nil {
		b.MetaCache.Delete(key)
	}
	return err
}

func (b *BaseDB) DeleteExpireKey(key []byte) error {
	wb := b.DB.GetExpireWriteBatchFromPool()
	defer b.DB.PutWriteBatchToPool(wb)

	_ = wb.Delete(key)
	return wb.Commit()
}

func (b *BaseDB) ClearBitmap(key []byte, deleteDB bool) (bool, error) {
	return b.BitmapMem.Delete(key, deleteDB)
}

func (b *BaseDB) SetMetaDataByValues(ek []byte, vlen int, value ...[]byte) error {
	wb := b.DB.GetMetaWriteBatchFromPool()
	defer b.DB.PutWriteBatchToPool(wb)

	_ = wb.PutMultiValue(ek, value...)
	err := wb.Commit()
	if err == nil && b.MetaCache != nil {
		b.MetaCache.PutMultiValue(ek, vlen, value...)
	}
	return err
}

func (b *BaseDB) GetAllDB() []kv.IKVStore {
	return b.DB.GetAllDB()
}

func (b *BaseDB) CacheInfo() string {
	if b.MetaCache == nil {
		return ""
	}
	memCap := b.MetaCache.MaxMem()
	usedMem := b.MetaCache.UsedMem()
	sahrdNum := b.MetaCache.Shards()
	effectiveMem := b.MetaCache.EffectiveMem()
	remainItemNum := b.MetaCache.Capacity()
	itemNum := b.MetaCache.Count()
	reputFailsCount := b.MetaCache.RePutFails()
	missCount := b.MetaCache.MissCount()
	queryCount := b.MetaCache.QueryCount()
	var hitRate float64
	if queryCount > 0 {
		hitRate = float64(queryCount-missCount) / float64(queryCount)
	}

	return fmt.Sprintf("shardNum:%d memCap:%d usedMem:%d effectiveMem:%d remainItem:%d Items:%d reputFailsCount:%d queryCount:%d missCount:%d hitRate:%.6f",
		sahrdNum, memCap, usedMem, effectiveMem, remainItemNum, itemNum, reputFailsCount, queryCount, missCount, hitRate)
}
