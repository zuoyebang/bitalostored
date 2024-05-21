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
	"path/filepath"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv/kv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/dbconfig"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
)

type BaseObject struct {
	BaseDb          *BaseDB
	DataDb          *bitskv.DB
	DataType        btools.DataType
	GetNextKeyId    func() uint64
	GetCurrentKeyId func() uint64
}

func NewBaseObject(baseDb *BaseDB, cfg *dbconfig.Config, dataType btools.DataType) BaseObject {
	bo := BaseObject{
		BaseDb:          baseDb,
		DataDb:          nil,
		DataType:        dataType,
		GetNextKeyId:    cfg.GetNextKeyId,
		GetCurrentKeyId: cfg.GetCurrentKeyId,
	}

	if dataType != btools.STRING {
		dataDb, err := bitskv.NewDataDB(cfg.DBPath, dataType, cfg)
		if err != nil {
			panic(err)
		}

		bo.DataDb = dataDb
	}

	return bo
}

func (bo *BaseObject) LockKey(khash uint32) func() {
	return bo.BaseDb.KeyLocker.LockWriteKey(khash)
}

func (bo *BaseObject) IsReady() bool {
	return bo.BaseDb.IsReady()
}

func (bo *BaseObject) Close() {
	if bo.DataDb != nil {
		bo.DataDb.Close()
	}
}

func (bo *BaseObject) CheckMetaData(mkv *MetaData) (isAlive bool, err error) {
	if mkv.IsAlive() {
		isAlive = true
		if mkv.dt != bo.DataType {
			err = errn.ErrWrongType
		}
	} else {
		mkv.Reuse(bo.DataType, bo.GetNextKeyId())
	}
	return isAlive, err
}

func (bo *BaseObject) GetMetaData(key []byte) (*MetaData, error) {
	return bo.BaseDb.getMetaWithoutValue(key, bo.DataType)
}

func (bo *BaseObject) GetMetaDataNoneType(key []byte) (*MetaData, error) {
	return bo.BaseDb.getMetaWithoutValue(key, btools.NoneType)
}

func (bo *BaseObject) GetMetaDataCheckAlive(key []byte, khash uint32) (*MetaData, error) {
	mk, mkCloser := EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := bo.GetMetaData(mk)
	if mkv == nil || err != nil {
		return nil, err
	}

	if !mkv.IsAlive() {
		PutMkvToPool(mkv)
		return nil, nil
	}

	return mkv, nil
}

func (bo *BaseObject) SetMetaData(ek []byte, mkv *MetaData) error {
	switch mkv.dt {
	case btools.STRING:
		var meta [MetaStringValueLen]byte
		EncodeMetaDbValueForString(meta[:], mkv.timestamp)
		vlen := MetaStringValueLen + len(mkv.value)
		return bo.SetMetaDataByValues(ek, vlen, meta[:], mkv.value)
	case btools.LIST:
		var meta [MetaListValueLen]byte
		EncodeMetaDbValueForList(meta[:], mkv)
		return bo.SetMetaDataByValue(ek, meta[:])
	default:
		var meta [MetaMixValueLen]byte
		EncodeMetaDbValueForMix(meta[:], mkv)
		return bo.SetMetaDataByValue(ek, meta[:])
	}
}

func (bo *BaseObject) SetMetaDataSize(ek []byte, khash uint32, delta int64) error {
	if delta == 0 {
		return nil
	}

	unlockKey := bo.LockKey(khash)
	defer unlockKey()

	mkv, err := bo.GetMetaData(ek)
	if err != nil {
		return err
	}
	defer PutMkvToPool(mkv)
	if !mkv.IsAlive() || mkv.dt == btools.STRING {
		return nil
	}

	if delta > 0 {
		mkv.size += uint32(delta)
	} else {
		size := uint32(-delta)
		if mkv.size <= size {
			mkv.size = 0
		} else {
			mkv.size -= size
		}
	}

	switch mkv.dt {
	case btools.ZSET, btools.SET, btools.HASH:
		var meta [MetaMixValueLen]byte
		EncodeMetaDbValueForMix(meta[:], mkv)
		return bo.SetMetaDataByValue(ek, meta[:])
	case btools.LIST:
		var meta [MetaListValueLen]byte
		EncodeMetaDbValueForList(meta[:], mkv)
		return bo.SetMetaDataByValue(ek, meta[:])
	default:
		return nil
	}
}

func (bo *BaseObject) SetMetaDataByValue(ek []byte, value []byte) error {
	wb := bo.GetMetaWriteBatchFromPool()
	defer bo.PutWriteBatchToPool(wb)

	_ = wb.Put(ek, value)
	err := wb.Commit()
	if err == nil && bo.BaseDb.MetaCache != nil {
		bo.BaseDb.MetaCache.Put(ek, value)
	}
	return err
}

func (bo *BaseObject) SetMetaDataByValues(ek []byte, vlen int, value ...[]byte) error {
	wb := bo.GetMetaWriteBatchFromPool()
	defer bo.PutWriteBatchToPool(wb)

	_ = wb.PutMultiValue(ek, value...)
	err := wb.Commit()
	if err == nil && bo.BaseDb.MetaCache != nil {
		bo.BaseDb.MetaCache.PutMultiValue(ek, vlen, value...)
	}
	return err
}

func (bo *BaseObject) UpdateExpire(oldKey, newKey []byte) error {
	wb := bo.GetExpireWriteBatchFromPool()
	defer bo.PutWriteBatchToPool(wb)

	if oldKey != nil {
		_ = wb.Delete(oldKey)
	}
	_ = wb.Put(newKey, NilDataVal)
	return wb.Commit()
}

func (bo *BaseObject) IsExistData(ek []byte) (bool, error) {
	return bo.DataDb.IsExistData(ek)
}

func (bo *BaseObject) GetDataValue(ekf []byte) ([]byte, bool, func(), error) {
	ekv, closer, err := bo.DataDb.GetData(ekf)
	if err != nil {
		if bo.DataDb.IsNotFound(err) {
			err = nil
		}
		if closer != nil {
			closer()
		}
		return nil, false, nil, err
	}

	return ekv, true, closer, nil
}

func (bo *BaseObject) GetDataWriteBatchFromPool() *bitskv.WriteBatch {
	return bo.DataDb.GetDataWriteBatchFromPool()
}

func (bo *BaseObject) GetIndexWriteBatchFromPool() *bitskv.WriteBatch {
	return bo.DataDb.GetIndexWriteBatchFromPool()
}

func (bo *BaseObject) GetMetaWriteBatchFromPool() *bitskv.WriteBatch {
	return bo.BaseDb.DB.GetMetaWriteBatchFromPool()
}

func (bo *BaseObject) GetExpireWriteBatchFromPool() *bitskv.WriteBatch {
	return bo.BaseDb.DB.GetExpireWriteBatchFromPool()
}

func (bo *BaseObject) PutWriteBatchToPool(wb *bitskv.WriteBatch) {
	bo.BaseDb.DB.PutWriteBatchToPool(wb)
}

func (bo *BaseObject) DataStats() bitskv.MetricsInfo {
	return bo.DataDb.DataStats()
}

func (bo *BaseObject) IndexStats() bitskv.MetricsInfo {
	return bo.DataDb.IndexStats()
}

func (bo *BaseObject) MetaStats() bitskv.MetricsInfo {
	return bo.BaseDb.DB.MetaStats()
}

func (bo *BaseObject) CheckpointDataDb(dir string) error {
	dir = filepath.Join(dir, bo.DataType.String())
	return bo.DataDb.Checkpoint(dir)
}

func (bo *BaseObject) GetAllDB() []kv.IKVStore {
	if bo.DataDb != nil {
		return bo.DataDb.GetAllDB()
	}
	return nil
}
