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

	"github.com/zuoyebang/bitalosdb/v2"
	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/engine/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

func (bdb *BitsDB) IsNotFound(err error) bool {
	return err == bitalosdb.ErrNotFound
}

func (bdb *BitsDB) Del(khash uint32, keys ...[]byte) (int64, error) {
	var isHashTag bool
	firstKeyHash := hash.Fnv32(keys[0])
	if firstKeyHash != khash {
		isHashTag = true
		firstKeyHash = utils.GetHashTagFnv(keys[0])
	}

	var n int64

	deletKey := func(key []byte, khash uint32) {
		if err := btools.CheckKeySize(key); err != nil {
			return
		}

		unlockKey := bdb.LockKey(khash)
		defer unlockKey()

		bitmapExist, _ := bdb.BitmapMem.Delete(key, true)
		if bitmapExist {
			n++
			return
		}

		res, _ := bdb.DeleteKey(key, khash)
		if res == 1 {
			n++
		}
	}

	for i, key := range keys {
		if i == 0 {
			khash = firstKeyHash
		} else if !isHashTag {
			khash = hash.Fnv32(key)
		}

		deletKey(key, khash)
	}

	return n, nil
}

func (bdb *BitsDB) Delete(key []byte, khash uint32) (int64, uint8, error) {
	return bdb.DB.Delete(key, btools.GetSlotId(khash))
}

func (bdb *BitsDB) expireAt(key []byte, khash uint32, timestamp uint64) (int64, error) {
	n, dataType, err := bdb.DB.ExpireAt(key, btools.GetSlotId(khash), timestamp)
	if err != nil {
		return 0, err
	}

	if dataType == btools.DataTypeString && bdb.StringCache != nil {
		bdb.StringCache.SetTimestamp(key, timestamp)
	}

	return n, nil
}

func (bdb *BitsDB) PExpireAt(key []byte, khash uint32, when int64) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	timestamp := uint64(when)

	unlockKey := bdb.LockKey(khash)
	defer unlockKey()

	if ret, ok := bdb.bitmapMemExpireAt(key, timestamp); ok {
		return ret, nil
	}

	return bdb.expireAt(key, khash, timestamp)
}

func (bdb *BitsDB) Persist(key []byte, khash uint32) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	unlockKey := bdb.LockKey(khash)
	defer unlockKey()

	if ret, ok := bdb.bitmapMemPersist(key); ok {
		return ret, nil
	}

	return bdb.expireAt(key, khash, 0)
}

func (bdb *BitsDB) PTTL(key []byte, khash uint32, p bool) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return -2, err
	}

	if ttl, ok := bdb.bitmapMemTTL(key); ok {
		if !p && ttl > 0 {
			ttl = tclock.SetTtlMilliToSec(ttl)
		}
		return ttl, nil
	}

	alive, ttl, _, _, _, _, _, _, err := bdb.GetMetaAlive(key, khash, btools.DataTypeNone)
	if !alive {
		return ttl, err
	}

	if !p && ttl > 0 {
		ttl = tclock.SetTtlMilliToSec(ttl)
	}
	return ttl, nil
}

func (bdb *BitsDB) Type(key []byte, khash uint32) (string, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return "none", err
	}

	alive, _, dt, _, _, _, _, _, err := bdb.GetMetaAlive(key, khash, btools.DataTypeNone)
	if !alive {
		return "none", err
	}

	dtString := btools.DataTypeNameMap[dt]
	return dtString, nil
}

func (bdb *BitsDB) Exists(key []byte, khash uint32) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	if ret, ok := bdb.bitmapMemExists(key); ok {
		return ret, nil
	}

	alive, _, _, _, _, _, _, _, err := bdb.GetMetaAlive(key, khash, btools.DataTypeNone)
	if !alive {
		return 0, err
	}

	return 1, nil
}

func (bdb *BitsDB) Length(key []byte, khash uint32, dt uint8) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	alive, _, _, _, _, _, _, size, err := bdb.GetMetaAlive(key, khash, dt)
	if !alive {
		return 0, err
	}

	return int64(size), nil
}

func (bdb *BitsDB) GetKV(key []byte, kslotid uint16) ([]byte, uint64, func(), error) {
	if bdb.StringCache != nil {
		val, closer, timestamp, exist := bdb.StringCache.Get(key)
		if exist {
			if bdb.enableMissCache && bytes.Equal(val, MissCacheValue) {
				closer()
				return nil, timestamp, nil, nil
			}
			return val, timestamp, closer, nil
		}
	}

	val, dataType, timestamp, closer, err := bdb.DB.Get(key, kslotid)
	if err != nil {
		if bdb.IsNotFound(err) {
			if bdb.enableMissCache {
				bdb.StringCache.RePut(key, MissCacheValue, timestamp)
			}
			err = nil
		} else {
			log.Errorf("GetKV fail key:%s err:%s", unsafe2.String(key), err)
			err = errn.ErrKvDb
		}
		return nil, 0, nil, err
	} else if dataType != btools.DataTypeString {
		if closer != nil {
			closer()
		}
		return nil, 0, nil, errn.ErrWrongType
	}

	if bdb.StringCache != nil && len(val) > 0 {
		bdb.StringCache.RePut(key, val, timestamp)
	}

	return val, timestamp, closer, nil
}

func (bdb *BitsDB) SetKV(key []byte, kslotid uint16, dt uint8, timestamp uint64, value []byte) error {
	err := bdb.DB.Set(key, kslotid, dt, timestamp, value)
	if err == nil && bdb.StringCache != nil {
		bdb.StringCache.Put(key, value, timestamp)
	}
	return err
}

func (bdb *BitsDB) DeleteKey(key []byte, khash uint32) (int64, error) {
	n, dataType, err := bdb.Delete(key, khash)
	if n == 1 && dataType == btools.DataTypeString && bdb.StringCache != nil {
		bdb.StringCache.Delete(key)
	}
	return n, err
}

func (bdb *BitsDB) GetMetaAlive(key []byte, khash uint32, dt uint8) (
	alive bool, ttl int64, dataType uint8,
	timestamp, version, lindex, rindex uint64, size uint32, err error,
) {
	dataType, timestamp, version, lindex, rindex, size, err = bdb.DB.GetMeta(key, btools.GetSlotId(khash))
	if err != nil {
		if bdb.IsNotFound(err) {
			ttl = ErrnoKeyNotFoundOrExpire
			err = nil
		}
		return
	}

	if dt > 0 && dt != dataType {
		err = ErrWrongType
		return
	}

	if timestamp == 0 {
		ttl = ErrnoKeyPersist
		alive = true
		return
	}

	ts := int64(timestamp)
	nowTime := tclock.GetTimestampMilli()
	if ts > nowTime {
		ttl = ts - nowTime
		alive = true
	} else {
		ttl = ErrnoKeyNotFoundOrExpire
	}

	return
}

func (bdb *BitsDB) DeleteKKV(key []byte, khash uint32, dt uint8, fields ...[]byte) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	unlockKey := bdb.LockKey(khash)
	defer unlockKey()

	return bdb.DB.DeleteKKV(key, btools.GetSlotId(khash), dt, fields...)
}

func (bdb *BitsDB) ListPush(key []byte, khash uint32, isLeft bool, isExist bool, values ...[]byte) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	unlockKey := bdb.LockKey(khash)
	defer unlockKey()

	return bdb.DB.ListPush(key, btools.GetSlotId(khash), isLeft, isExist, values...)
}

func (bdb *BitsDB) ListPop(key []byte, khash uint32, isLeft bool) ([]byte, func(), error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, nil, err
	}

	unlockKey := bdb.LockKey(khash)
	defer unlockKey()

	return bdb.DB.ListPop(key, btools.GetSlotId(khash), isLeft)
}
