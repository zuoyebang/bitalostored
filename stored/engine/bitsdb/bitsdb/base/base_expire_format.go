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
	"encoding/binary"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/bytepools"
)

const (
	DeleteMixKeyMaxNum   = 20000
	DeleteMixFieldMaxNum = 1000
)

func EncodeExpireKey(key []byte, mkv *MetaData) ([]byte, func()) {
	size := expireKeyHeaderLength + len(key)
	pool, closer := bytepools.BytePools.GetBytePool(size)

	pos := keyTimestampLength
	binary.BigEndian.PutUint64(pool[0:pos], mkv.timestamp)
	pool[pos] = uint8(mkv.dt)
	pos += keyDataTypeLength
	binary.BigEndian.PutUint64(pool[pos:], mkv.version)
	pos += keyVersionLength
	copy(pool[pos:], key)

	return pool[:size], closer
}

func DecodeExpireKey(ek []byte) (timestamp uint64, dt btools.DataType, version uint64, kind uint8, key []byte, err error) {
	if len(ek) <= expireKeyStringHeaderLength {
		return 0, 0, 0, 0, nil, errEncodeKVKey
	}

	timestamp = binary.BigEndian.Uint64(ek[0:])
	pos := keyTimestampLength
	dt = btools.DataType(ek[pos])
	pos += keyDataTypeLength
	if len(ek) <= expireKeyHeaderLength {
		return 0, 0, 0, 0, nil, errEncodeKVKey
	}
	version = binary.BigEndian.Uint64(ek[pos:])
	kind = DecodeKeyVersionKind(version)
	pos += keyVersionLength

	key = ek[pos:]

	return timestamp, dt, version, kind, key, nil
}

func (bo *BaseObject) DeleteDataKeyByExpire(keyVersion uint64, keyHash uint32) error {
	var keyBuf [DataKeyHeaderLength]byte
	PutDataKeyHeader(keyBuf[:], keyVersion, keyHash)

	wb := bo.GetDataWriteBatchFromPool()
	defer bo.PutWriteBatchToPool(wb)
	_ = wb.PutPrefixDeleteKey(keyBuf[:])
	return wb.Commit()
}

func (bo *BaseObject) DeleteZsetIndexKeyByExpire(keyVersion uint64, keyHash uint32) error {
	var keyBuf [DataKeyHeaderLength]byte
	PutDataKeyHeader(keyBuf[:], keyVersion, keyHash)

	wb := bo.GetIndexWriteBatchFromPool()
	defer bo.PutWriteBatchToPool(wb)
	_ = wb.PutPrefixDeleteKey(keyBuf[:])
	return wb.Commit()
}

func (bo *BaseObject) DeleteZsetOldKeyByExpire(keyVersion uint64, keyKind uint8, khash uint32) (bool, uint64, error) {
	var cnt uint64
	var dataKey [DataKeyZsetLength]byte
	var lowerBound [DataKeyHeaderLength]byte
	var upperBound [DataKeyUpperBoundLength]byte
	EncodeDataKeyLowerBound(lowerBound[:], keyVersion, khash)
	EncodeDataKeyUpperBound(upperBound[:], keyVersion, khash)
	iterOpts := &bitskv.IterOptions{
		KeyHash:      khash,
		UpperBound:   upperBound[:],
		DisableCache: true,
	}
	it := bo.DataDb.NewIteratorIndex(iterOpts)
	indexWb := bo.GetIndexWriteBatchFromPool()
	dataWb := bo.GetDataWriteBatchFromPool()
	defer func() {
		it.Close()
		bo.PutWriteBatchToPool(indexWb)
		bo.PutWriteBatchToPool(dataWb)
	}()

	for it.Seek(lowerBound[:]); it.Valid() && it.ValidForPrefix(lowerBound[:]); it.Next() {
		indexKey := it.RawKey()
		_ = indexWb.Delete(indexKey)
		_, _, fp := DecodeZsetIndexKey(keyKind, indexKey, it.RawValue())
		dataKeyLen := EncodeZsetDataKey(dataKey[:], keyVersion, khash, fp.Merge(), true)
		_ = dataWb.Delete(dataKey[:dataKeyLen])
		cnt++
		if cnt >= DeleteMixFieldMaxNum {
			break
		}
	}

	if cnt == 0 {
		return true, 0, nil
	}
	if err := dataWb.Commit(); err != nil {
		return false, 0, err
	}
	if err := indexWb.Commit(); err != nil {
		return false, 0, err
	}

	return cnt < DeleteMixFieldMaxNum, cnt, nil
}
