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

package set

import (
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
)

func (so *SetObject) SAdd(key []byte, khash uint32, args ...[]byte) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	var members [][]byte
	argsNum := len(args)
	if argsNum <= 1 {
		members = args
	} else {
		uniqMembers := make(map[string]bool, argsNum)
		members = make([][]byte, 0, argsNum)
		for i := range args {
			if _, ok := uniqMembers[unsafe2.String(args[i])]; !ok {
				uniqMembers[unsafe2.String(args[i])] = true
				members = append(members, args[i])
			}
		}
	}

	unlockKey := so.LockKey(khash)
	defer unlockKey()

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := so.GetMetaDataNoneType(mk)
	if err != nil {
		return 0, err
	}
	defer base.PutMkvToPool(mkv)

	if _, err = so.CheckMetaData(mkv); err != nil {
		return 0, err
	}

	wb := so.GetDataWriteBatchFromPool()
	defer so.PutWriteBatchToPool(wb)
	var n int64
	keyVersion := mkv.Version()
	keyKind := mkv.Kind()
	for i := 0; i < len(members); i++ {
		if err = btools.CheckFieldSize(members[i]); err != nil {
			continue
		}

		ekf, ekfCloser, isCompress := base.EncodeSetDataKey(keyVersion, keyKind, khash, members[i])
		if exist, e := so.IsExistData(ekf); e == nil && !exist {
			_ = base.SetDataValue(wb, ekf, members[i], isCompress)
			n++
			mkv.IncrSize(1)
		}
		ekfCloser()
	}

	if n > 0 {
		if err = wb.Commit(); err != nil {
			return 0, err
		}
		if err = so.SetMetaData(mk, mkv); err != nil {
			return 0, err
		}
	}

	return n, nil
}

func (so *SetObject) SRem(key []byte, khash uint32, args ...[]byte) (int64, error) {
	return so.BaseDeleteDataValue(key, khash, args...)
}

func (so *SetObject) SPop(key []byte, khash uint32, count int64) ([][]byte, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, err
	}

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := so.GetMetaData(mk)
	if err != nil {
		return nil, err
	}
	defer base.PutMkvToPool(mkv)
	if !mkv.IsAlive() {
		return nil, nil
	}

	var delCnt int64
	var lowerBound [base.DataKeyHeaderLength]byte
	var upperBound [base.DataKeyUpperBoundLength]byte
	wb := so.GetDataWriteBatchFromPool()
	keyVersion := mkv.Version()
	keyKind := mkv.Kind()
	stopCnt := mkv.Size()
	members := make([][]byte, 0, count)
	base.EncodeDataKeyLowerBound(lowerBound[:], keyVersion, khash)
	base.EncodeDataKeyUpperBound(upperBound[:], keyVersion, khash)
	iterOpts := &bitskv.IterOptions{
		KeyHash:    khash,
		UpperBound: upperBound[:],
	}
	it := so.DataDb.NewIterator(iterOpts)
	defer func() {
		it.Close()
		so.PutWriteBatchToPool(wb)
	}()
	for it.Seek(lowerBound[:]); it.Valid(); it.Next() {
		itKey := it.RawKey()
		version, fp := base.DecodeSetDataKey(keyKind, itKey, it.RawValue())
		if version != keyVersion {
			break
		}

		members = append(members, fp.Merge())
		delCnt++
		_ = wb.Delete(itKey)
		if delCnt == count || delCnt >= stopCnt {
			break
		}
	}

	if delCnt > 0 {
		if err = wb.Commit(); err != nil {
			return nil, err
		}
		if err = so.SetMetaDataSize(mk, khash, -delCnt); err != nil {
			return nil, err
		}
	}

	return members, nil
}
