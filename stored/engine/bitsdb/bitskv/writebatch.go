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

package bitskv

import (
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv/kv"
)

func (db *DB) GetDataWriteBatchFromPool() *WriteBatch {
	wb := &WriteBatch{
		db: db,
		wb: db.dataDb.GetWriteBatch(),
	}
	return wb
}

func (db *DB) GetMetaWriteBatchFromPool() *WriteBatch {
	wb := &WriteBatch{
		db: db,
		wb: db.metaDb.GetWriteBatch(),
	}
	return wb
}

func (db *DB) GetIndexWriteBatchFromPool() *WriteBatch {
	wb := &WriteBatch{
		db: db,
		wb: db.indexDb.GetWriteBatch(),
	}
	return wb
}

func (db *DB) GetExpireWriteBatchFromPool() *WriteBatch {
	wb := &WriteBatch{
		db: db,
		wb: db.expireDb.GetWriteBatch(),
	}
	return wb
}

func (db *DB) PutWriteBatchToPool(wb *WriteBatch) {
	_ = wb.Close()
}

type WriteBatch struct {
	db *DB
	wb kv.IWriteBatch
}

func (wb *WriteBatch) Close() error {
	err := wb.wb.Close()
	wb.wb = nil
	return err
}

func (wb *WriteBatch) Put(key []byte, value []byte) error {
	return wb.wb.Put(key, value)
}

func (wb *WriteBatch) PutMultiValue(key []byte, vals ...[]byte) error {
	return wb.wb.PutMultiValue(key, vals...)
}

func (wb *WriteBatch) PutPrefixDeleteKey(key []byte) error {
	return wb.wb.PutPrefixDeleteKey(key)
}

func (wb *WriteBatch) Delete(key []byte) error {
	return wb.wb.Delete(key)
}

func (wb *WriteBatch) Commit() error {
	defer wb.Clear()
	return wb.wb.Commit()
}

func (wb *WriteBatch) Count() int {
	return wb.wb.Count()
}

func (wb *WriteBatch) Clear() {
	wb.wb.Clear()
}
