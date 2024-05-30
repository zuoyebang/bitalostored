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

package hash

import (
	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
)

func (ho *HashObject) HSet(key []byte, khash uint32, field []byte, value []byte) (int64, error) {
	if err := btools.CheckKeyAndFieldSize(key, field); err != nil {
		return 0, err
	} else if err = btools.CheckValueSize(value); err != nil {
		return 0, err
	}

	unlockKey := ho.LockKey(khash)
	defer unlockKey()

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := ho.GetMetaDataNoneType(mk)
	if err != nil {
		return 0, err
	}
	defer base.PutMkvToPool(mkv)

	if _, err = ho.CheckMetaData(mkv); err != nil {
		return 0, err
	}

	ekf, ekfCloser := base.EncodeDataKey(mkv.Version(), khash, field)
	defer ekfCloser()
	hfexist, err := ho.IsExistData(ekf)
	if err != nil {
		return 0, err
	}

	if !hfexist {
		mkv.IncrSize(1)
	}

	nwb := ho.GetDataWriteBatchFromPool()
	defer ho.PutWriteBatchToPool(nwb)
	_ = nwb.Put(ekf, value)
	if err = nwb.Commit(); err != nil {
		return 0, err
	}

	if hfexist {
		return 0, nil
	}
	if err = ho.SetMetaData(mk, mkv); err != nil {
		return 0, err
	}
	return 1, nil
}

func (ho *HashObject) HMset(key []byte, khash uint32, args ...btools.FVPair) error {
	if err := btools.CheckKeySize(key); err != nil {
		return err
	}

	unlockKey := ho.LockKey(khash)
	defer unlockKey()

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := ho.GetMetaDataNoneType(mk)
	if err != nil {
		return err
	}
	defer base.PutMkvToPool(mkv)

	if _, err = ho.CheckMetaData(mkv); err != nil {
		return err
	}

	wb := ho.GetDataWriteBatchFromPool()
	defer ho.PutWriteBatchToPool(wb)
	var n int64
	var isWbPut, hfexist bool
	keyVersion := mkv.Version()
	for i := 0; i < len(args); i++ {
		if err = btools.CheckFieldSize(args[i].Field); err != nil {
			continue
		} else if err = btools.CheckValueSize(args[i].Value); err != nil {
			continue
		}

		ekf, ekfCloser := base.EncodeDataKey(keyVersion, khash, args[i].Field)
		hfexist, err = ho.IsExistData(ekf)
		if err == nil {
			if !hfexist {
				n++
				mkv.IncrSize(1)
			}

			_ = wb.Put(ekf, args[i].Value)
			if !isWbPut {
				isWbPut = true
			}
		}
		ekfCloser()
	}

	if isWbPut {
		if err = wb.Commit(); err != nil {
			return err
		}
	}
	if n > 0 {
		if err = ho.SetMetaData(mk, mkv); err != nil {
			return err
		}
	}

	return nil
}

func (ho *HashObject) HDel(key []byte, khash uint32, args ...[]byte) (int64, error) {
	return ho.BaseDeleteDataValue(key, khash, args...)
}

func (ho *HashObject) HIncrBy(key []byte, khash uint32, field []byte, delta int64) (int64, error) {
	if err := btools.CheckKeyAndFieldSize(key, field); err != nil {
		return 0, err
	}

	unlockKey := ho.LockKey(khash)
	defer unlockKey()

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := ho.GetMetaDataNoneType(mk)
	if err != nil {
		return 0, err
	}
	defer base.PutMkvToPool(mkv)

	hkexist, err := ho.CheckMetaData(mkv)
	if err != nil {
		return 0, err
	}

	wb := ho.GetDataWriteBatchFromPool()
	defer ho.PutWriteBatchToPool(wb)

	var n int64
	var isMetaUpdate bool

	ekf, ekfCloser := base.EncodeDataKey(mkv.Version(), khash, field)
	defer ekfCloser()

	if hkexist {
		value, hfexist, valCloser, err := ho.GetDataValue(ekf)
		defer func() {
			if valCloser != nil {
				valCloser()
			}
		}()
		if err != nil {
			return 0, err
		}
		if hfexist {
			if n, err = btools.StrInt64(value, err); err != nil {
				return 0, err
			}
			if delta == 0 {
				return n, nil
			}
			n += delta
		} else {
			n = delta
			isMetaUpdate = true
		}
	} else {
		n += delta
		isMetaUpdate = true
	}

	_ = wb.Put(ekf, extend.FormatInt64ToSlice(n))
	if err = wb.Commit(); err != nil {
		return 0, err
	}

	if isMetaUpdate {
		mkv.IncrSize(1)
		if err = ho.SetMetaData(mk, mkv); err != nil {
			return 0, err
		}
	}

	return n, nil
}
