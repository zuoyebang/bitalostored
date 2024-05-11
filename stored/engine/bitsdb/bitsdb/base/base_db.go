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

package base

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
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
	cacheBucketNum   int = 1024
	eliminateThreads int = 1
)

type BaseDB struct {
	DB            *bitskv.DB
	MetaCache     *vectormap.VectorMap
	DelExpirePool *ants.PoolWithFunc
	IsKeyScan     atomic.Int32
	Ready         atomic.Bool
	KeyLocker     *locker.ScopeLocker
}

func NewBaseDB(cfg *dbconfig.Config) (*BaseDB, error) {
	db, err := bitskv.NewBaseDB(cfg)
	if err != nil {
		return nil, err
	}

	baseDb := &BaseDB{
		DB:        db,
		KeyLocker: locker.NewScopeLocker(btools.KeyLockerPoolCap),
		MetaCache: nil,
	}

	if cfg.CacheSize > 0 {
		baseDb.MetaCache = vectormap.NewVectorMap(uint32(cfg.CacheHashSize),
			vectormap.WithBuckets(cacheBucketNum),
			vectormap.WithLogger(log.GetLogger()),
			vectormap.WithEliminate(vectormap.Byte(cfg.CacheSize), eliminateThreads, 180*time.Second))
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
		b.MetaCache = nil
	}
}

func (b *BaseDB) ClearCache() {
	if b.MetaCache != nil {
		b.MetaCache.Clear()
	}
}

func (b *BaseDB) GetMeta(key []byte) ([]byte, func(), error) {
	if b.MetaCache != nil {
		v, closer, ok := b.MetaCache.Get(key)
		if ok {
			return v, closer, nil
		}
	}

	val, closer, err := b.DB.GetMeta(key)
	if b.DB.IsNotFound(err) {
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

	if dt != btools.NoneType && dt != mkv.dt {
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

func (b *BaseDB) DeleteMetaKeyByExpire(
	dt btools.DataType, key []byte, khash uint32, keyVersion uint64, expireTime uint64,
) (bool, error) {
	var isDel bool
	mk, mkCloser := EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := b.getMetaWithoutValue(mk, dt)
	if mkv == nil {
		return isDel, err
	}
	defer PutMkvToPool(mkv)

	if dt == btools.STRING {
		if mkv.timestamp == expireTime {
			isDel = true
		}
	} else if mkv.version <= keyVersion && mkv.timestamp > 0 && mkv.timestamp <= expireTime {
		isDel = true
	}

	if isDel {
		return isDel, b.DeleteMetaKey(mk)
	}

	return isDel, nil
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

func (b *BaseDB) SetDelExpireDataPool(pool *ants.PoolWithFunc) {
	b.DelExpirePool = pool
}

func (b *BaseDB) GetAllDB() []kv.IKVStore {
	return b.DB.GetAllDB()
}

func (b *BaseDB) CacheInfo() string {
	if b.MetaCache == nil {
		return ""
	}
	maxMem := b.MetaCache.MaxMem()
	usedMem := b.MetaCache.UsedMem()
	effectiveMem := b.MetaCache.EffectiveMem()
	remainItemNum := b.MetaCache.Capacity()
	itemNum := b.MetaCache.Count()
	missCount := b.MetaCache.MissCount()
	queryCount := b.MetaCache.QueryCount()
	var hitRate float64
	if queryCount > 0 {
		hitRate = float64(queryCount-missCount) / float64(queryCount)
	}
	return fmt.Sprintf("maxmem:%d usedMem:%d effectiveMem:%d remainItem:%d items:%d queryCount:%d missCount:%d hitRate:%.4f", maxMem, usedMem, effectiveMem, remainItemNum, itemNum, queryCount, missCount, hitRate)
}
