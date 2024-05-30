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
	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

func (bo *BaseObject) Del(khash uint32, keys ...[]byte) (n int64, err error) {
	var isHashTag bool
	firstKeyHash := hash.Fnv32(keys[0])
	if firstKeyHash != khash {
		isHashTag = true
		firstKeyHash = utils.GetHashTagFnv(keys[0])
	}

	for i, key := range keys {
		if i == 0 {
			khash = firstKeyHash
		} else if !isHashTag {
			khash = hash.Fnv32(key)
		}
		func(key []byte, khash uint32) {
			if err = btools.CheckKeySize(key); err != nil {
				return
			}

			unlockKey := bo.LockKey(khash)
			defer unlockKey()

			mk, mkCloser := EncodeMetaKey(key, khash)
			defer mkCloser()
			mkv, err := bo.BaseDb.BaseGetMetaWithoutValue(mk)
			if err != nil {
				return
			}
			defer PutMkvToPool(mkv)
			if !mkv.IsAlive() {
				return
			}

			if mkv.dt == btools.STRING {
				if err := bo.BaseDb.DeleteMetaKey(mk); err != nil {
					return
				}
			} else {
				oldExpireKey, oekCloser := EncodeExpireKey(key, mkv)
				mkv.Del()
				newExpireKey, nekCloser := EncodeExpireKey(key, mkv)
				defer func() {
					oekCloser()
					nekCloser()
				}()
				if err := bo.SetMetaData(mk, mkv); err != nil {
					return
				}
				if err := bo.UpdateExpire(oldExpireKey, newExpireKey); err != nil {
					return
				}
			}

			n++
		}(key, khash)
	}
	return n, err
}

func (bo *BaseObject) Expire(key []byte, khash uint32, duration int64) (int64, error) {
	if duration <= 0 {
		return bo.Del(khash, key)
	}

	when := tclock.GetTimestampSecond() + duration
	return bo.BaseExpireAt(key, khash, tclock.SetTimestampMilli(when))
}

func (bo *BaseObject) PExpire(key []byte, khash uint32, duration int64) (int64, error) {
	if duration <= 0 {
		return bo.Del(khash, key)
	}

	when := tclock.GetTimestampMilli() + duration
	return bo.BaseExpireAt(key, khash, when)
}

func (bo *BaseObject) ExpireAt(key []byte, khash uint32, when int64) (int64, error) {
	if when <= tclock.GetTimestampSecond() {
		return bo.Del(khash, key)
	}

	return bo.BaseExpireAt(key, khash, tclock.SetTimestampMilli(when))
}

func (bo *BaseObject) PExpireAt(key []byte, khash uint32, when int64) (int64, error) {
	if when <= tclock.GetTimestampMilli() {
		return bo.Del(khash, key)
	}

	return bo.BaseExpireAt(key, khash, when)
}

func (bo *BaseObject) BaseExpireAt(key []byte, khash uint32, when int64) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	unlockKey := bo.LockKey(khash)
	defer unlockKey()

	mk, mkCloser := EncodeMetaKey(key, khash)
	mkv, mvCloser, err := bo.BaseDb.BaseGetMetaWithValue(mk)
	defer func() {
		mkCloser()
		if mvCloser != nil {
			mvCloser()
		}
	}()
	if mkv == nil {
		return 0, err
	}
	defer PutMkvToPool(mkv)
	if !mkv.IsAlive() {
		return 0, nil
	}

	if mkv.dt == btools.STRING {
		mkv.SetTimestamp(uint64(when))
		if err = bo.SetMetaData(mk, mkv); err != nil {
			return 0, err
		}
	} else {
		oldExpireKey, oekCloser := EncodeExpireKey(key, mkv)
		mkv.SetTimestamp(uint64(when))
		newExpireKey, nekCloser := EncodeExpireKey(key, mkv)
		defer func() {
			oekCloser()
			nekCloser()
		}()

		if err = bo.SetMetaData(mk, mkv); err != nil {
			return 0, err
		}

		err = bo.UpdateExpire(oldExpireKey, newExpireKey)
		if err != nil {
			return 0, err
		}
	}

	return 1, nil
}

func (bo *BaseObject) BasePTTL(key []byte, khash uint32, p bool) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return -2, err
	}

	mk, mkCloser := EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := bo.BaseDb.BaseGetMetaWithoutValue(mk)
	if err != nil {
		return 0, err
	}

	defer PutMkvToPool(mkv)
	ttl := mkv.CheckTTL()
	if !p && ttl > 0 {
		ttl = tclock.SetTtlMilliToSec(ttl)
	}

	return ttl, nil
}

func (bo *BaseObject) BaseType(key []byte, khash uint32) (string, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return "none", err
	}

	mkv, err := bo.BaseDb.BaseGetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return "none", err
	}
	defer PutMkvToPool(mkv)

	return mkv.dt.String(), nil
}

func (bo *BaseObject) BaseExists(key []byte, khash uint32) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	mkv, err := bo.BaseDb.BaseGetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return 0, err
	}
	defer PutMkvToPool(mkv)

	return 1, nil
}

func (bo *BaseObject) BasePersist(key []byte, khash uint32) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	unlockKey := bo.LockKey(khash)
	defer unlockKey()

	mk, mkCloser := EncodeMetaKey(key, khash)
	mkv, mvCloser, err := bo.BaseDb.BaseGetMetaWithValue(mk)
	defer func() {
		mkCloser()
		if mvCloser != nil {
			mvCloser()
		}
	}()
	if mkv == nil {
		return 0, err
	}
	defer PutMkvToPool(mkv)

	if mkv.CheckTTL() <= ErrnoKeyPersist {
		return 0, nil
	}

	expireKey, oekCloser := EncodeExpireKey(key, mkv)
	defer oekCloser()

	mkv.Persist()

	if err = bo.SetMetaData(mk, mkv); err != nil {
		return 0, err
	}

	if err = bo.BaseDb.DeleteExpireKey(expireKey); err != nil {
		return 0, err
	}

	return 1, nil
}

func (bo *BaseObject) BaseSize(key []byte, khash uint32) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	mkv, err := bo.GetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return 0, err
	}
	defer PutMkvToPool(mkv)

	return mkv.Size(), nil
}

func (bo *BaseObject) BaseDeleteDataValue(key []byte, khash uint32, args ...[]byte) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	mk, mkCloser := EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := bo.GetMetaData(mk)
	if err != nil {
		return 0, err
	}
	defer PutMkvToPool(mkv)
	if !mkv.IsAlive() {
		return 0, nil
	}

	var (
		delCnt int64
		exist  bool
	)

	wb := bo.GetDataWriteBatchFromPool()
	defer bo.PutWriteBatchToPool(wb)
	keyVersion := mkv.Version()
	for i := 0; i < len(args); i++ {
		if err = btools.CheckFieldSize(args[i]); err != nil {
			continue
		}
		ekf, ekfCloser := EncodeDataKey(keyVersion, khash, args[i])
		exist, _ = bo.IsExistData(ekf)
		if exist {
			delCnt++
			_ = wb.Delete(ekf)
		}
		if ekfCloser != nil {
			ekfCloser()
		}
	}

	if delCnt > 0 {
		if err = wb.Commit(); err != nil {
			return 0, err
		}
		if err = bo.SetMetaDataSize(mk, khash, -delCnt); err != nil {
			return 0, err
		}
	}

	return delCnt, nil
}
