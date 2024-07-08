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

package zset

import (
	"bytes"

	"github.com/zuoyebang/bitalostored/butils/numeric"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
)

func setZsetOldDataType(mkv *base.MetaData) {
	if mkv.GetDataType() != btools.ZSETOLD {
		mkv.SetDataType(btools.ZSETOLD)
	}
}

func (zo *ZSetObject) ZAdd(key []byte, khash uint32, isOld bool, args ...btools.ScorePair) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	argsNum := len(args)
	if argsNum == 0 {
		return 0, errn.ErrArgsEmpty
	}

	unlockKey := zo.LockKey(khash)
	defer unlockKey()

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := zo.GetMetaDataNoneType(mk)
	if err != nil {
		return 0, err
	}
	defer base.PutMkvToPool(mkv)

	if _, err = zo.CheckMetaData(mkv); err != nil {
		return 0, err
	}

	if isOld {
		setZsetOldDataType(mkv)
	}

	dataWb := zo.GetDataWriteBatchFromPool()
	defer zo.PutWriteBatchToPool(dataWb)
	indexWb := zo.GetIndexWriteBatchFromPool()
	defer zo.PutWriteBatchToPool(indexWb)

	var count int64
	var scoreBuf [base.ScoreLength]byte
	var ekfBuf [base.DataKeyZsetLength]byte
	keyVersion := mkv.Version()
	keyKind := mkv.Kind()
	isZsetOld := mkv.IsZsetOld()

	zadd := func(score float64, member []byte) error {
		if e := btools.CheckFieldSize(member); e != nil {
			return e
		}

		ekfLen := base.EncodeZsetDataKey(ekfBuf[:], keyVersion, khash, member, isZsetOld)
		ekf := ekfBuf[:ekfLen]
		value, exist, closer, e := zo.GetDataValue(ekf)
		if e != nil {
			return e
		}
		defer func() {
			if closer != nil {
				closer()
			}
		}()

		if !exist {
			count++
			mkv.IncrSize(1)
		} else {
			oldScore := numeric.ByteSortToFloat64(value)
			if oldScore == score {
				return nil
			}
			zo.deleteZsetIndexKey(indexWb, keyVersion, keyKind, khash, oldScore, member)
		}

		dataWb.Put(ekf, numeric.Float64ToByteSort(score, scoreBuf[:]))
		zo.setZsetIndexValue(indexWb, keyVersion, keyKind, khash, score, member)

		return nil
	}

	argsDup := make(map[string]struct{}, argsNum)
	for i := range args {
		member := unsafe2.String(args[i].Member)
		if _, exist := argsDup[member]; exist {
			continue
		}
		if err = zadd(args[i].Score, args[i].Member); err != nil {
			return 0, err
		}
		argsDup[member] = struct{}{}
	}

	if err = dataWb.Commit(); err != nil {
		return 0, err
	}
	if err = indexWb.Commit(); err != nil {
		return 0, err
	}
	if count > 0 {
		if err = zo.SetMetaData(mk, mkv); err != nil {
			return 0, err
		}
	}

	return count, err
}

func (zo *ZSetObject) ZIncrBy(key []byte, khash uint32, isOld bool, delta float64, member []byte) (float64, error) {
	if err := btools.CheckKeyAndFieldSize(key, member); err != nil {
		return 0, err
	}

	unlockKey := zo.LockKey(khash)
	defer unlockKey()

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := zo.GetMetaDataNoneType(mk)
	if err != nil {
		return 0, err
	}
	defer base.PutMkvToPool(mkv)

	kexist := mkv.IsAlive()
	if !kexist {
		mkv.Reuse(zo.DataType, zo.GetNextKeyId())
	}

	if isOld {
		setZsetOldDataType(mkv)
	}

	dataWb := zo.GetDataWriteBatchFromPool()
	defer zo.PutWriteBatchToPool(dataWb)
	indexWb := zo.GetIndexWriteBatchFromPool()
	defer zo.PutWriteBatchToPool(indexWb)
	metaWb := zo.GetMetaWriteBatchFromPool()
	defer zo.PutWriteBatchToPool(metaWb)

	var newScore float64
	var scoreBuf [base.ScoreLength]byte
	var ekfBuf [base.DataKeyZsetLength]byte
	keyVersion := mkv.Version()
	keyKind := mkv.Kind()
	isZsetOld := mkv.IsZsetOld()
	ekfLen := base.EncodeZsetDataKey(ekfBuf[:], keyVersion, khash, member, isZsetOld)
	ekf := ekfBuf[:ekfLen]

	var updateCache func() = nil

	if !kexist {
		mkv.IncrSize(1)
		newScore = delta
		var meta [base.MetaMixValueLen]byte
		base.EncodeMetaDbValueForMix(meta[:], mkv)
		metaWb.Put(mk, meta[:])
		updateCache = func() {
			if zo.BaseDb.MetaCache != nil {
				zo.BaseDb.MetaCache.Put(mk, meta[:])
			}
		}

		dataWb.Put(ekf, numeric.Float64ToByteSort(delta, scoreBuf[:]))
		zo.setZsetIndexValue(indexWb, keyVersion, keyKind, khash, newScore, member)
	} else {
		value, mbexist, valCloser, e := zo.GetDataValue(ekf)
		defer func() {
			if valCloser != nil {
				valCloser()
			}
		}()
		if e != nil {
			return 0, e
		}
		oldScore := float64(0)
		if mbexist {
			oldScore = numeric.ByteSortToFloat64(value)
			if delta == 0 {
				return oldScore, nil
			}
		} else {
			mkv.IncrSize(1)
			var meta [base.MetaMixValueLen]byte
			base.EncodeMetaDbValueForMix(meta[:], mkv)
			metaWb.Put(mk, meta[:])
			updateCache = func() {
				if zo.BaseDb.MetaCache != nil {
					zo.BaseDb.MetaCache.Put(mk, meta[:])
				}
			}
		}
		zo.deleteZsetIndexKey(indexWb, keyVersion, keyKind, khash, oldScore, member)
		newScore = oldScore + delta
		dataWb.Put(ekf, numeric.Float64ToByteSort(newScore, scoreBuf[:]))
		zo.setZsetIndexValue(indexWb, keyVersion, keyKind, khash, newScore, member)
	}

	if err = dataWb.Commit(); err != nil {
		return 0, err
	}
	if err = indexWb.Commit(); err != nil {
		return 0, err
	}
	if err = metaWb.Commit(); err != nil {
		return 0, err
	} else if updateCache != nil {
		updateCache()
	}

	return newScore, nil
}

func (zo *ZSetObject) ZRem(key []byte, khash uint32, members ...[]byte) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	if len(members) == 0 {
		return 0, nil
	}

	unlockKey := zo.LockKey(khash)
	defer unlockKey()

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := zo.GetMetaData(mk)
	if err != nil {
		return 0, err
	}
	defer base.PutMkvToPool(mkv)
	if !mkv.IsAlive() {
		return 0, nil
	}

	dataWb := zo.GetDataWriteBatchFromPool()
	defer zo.PutWriteBatchToPool(dataWb)
	indexWb := zo.GetIndexWriteBatchFromPool()
	defer zo.PutWriteBatchToPool(indexWb)

	var count int64
	var ekf [base.DataKeyZsetLength]byte

	keyVersion := mkv.Version()
	keyKind := mkv.Kind()
	isZsetOld := mkv.IsZsetOld()

	zrem := func(member []byte) error {
		if e := btools.CheckFieldSize(member); e != nil {
			return e
		}

		ekfLen := base.EncodeZsetDataKey(ekf[:], keyVersion, khash, member, isZsetOld)
		value, exist, closer, e := zo.GetDataValue(ekf[:ekfLen])
		if e != nil {
			return e
		}
		defer func() {
			if closer != nil {
				closer()
			}
		}()

		if exist {
			count++
			mkv.DecrSize(1)
			dataWb.Delete(ekf[:ekfLen])
			zo.deleteZsetIndexKey(indexWb, keyVersion, keyKind, khash, numeric.ByteSortToFloat64(value), member)
		}

		return nil
	}

	for i := range members {
		if err = zrem(members[i]); err != nil {
			return 0, err
		}
	}

	if count > 0 {
		if err = dataWb.Commit(); err != nil {
			return 0, err
		}
		if err = indexWb.Commit(); err != nil {
			return 0, err
		}
		if err = zo.SetMetaData(mk, mkv); err != nil {
			return 0, err
		}
	}
	return count, err
}

func (zo *ZSetObject) ZRemRangeByRank(key []byte, khash uint32, start int64, stop int64) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := zo.GetMetaData(mk)
	if err != nil {
		return 0, err
	}
	defer base.PutMkvToPool(mkv)
	if !mkv.IsAlive() {
		return 0, nil
	}

	size := mkv.Size()
	startIndex, stopIndex := zo.zParseLimit(size, start, stop, false)
	if startIndex > stopIndex || startIndex >= size || stopIndex < 0 {
		return 0, nil
	}

	dataWb := zo.GetDataWriteBatchFromPool()
	defer zo.PutWriteBatchToPool(dataWb)
	indexWb := zo.GetIndexWriteBatchFromPool()
	defer zo.PutWriteBatchToPool(indexWb)

	var index, delCnt int64
	var dataKey [base.DataKeyZsetLength]byte
	var lowerBound [base.DataKeyHeaderLength]byte
	var upperBound [base.IndexKeyScoreLength]byte

	keyVersion := mkv.Version()
	keyKind := mkv.Kind()
	isZsetOld := mkv.IsZsetOld()
	base.EncodeDataKeyLowerBound(lowerBound[:], keyVersion, khash)
	base.EncodeZsetIndexKeyUpperBound(upperBound[:], keyVersion, khash)
	iterOpts := &bitskv.IterOptions{
		KeyHash:    khash,
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
	}
	it := zo.DataDb.NewIteratorIndex(iterOpts)
	defer it.Close()

	for it.Seek(lowerBound[:]); it.Valid(); it.Next() {
		if index >= startIndex {
			indexKey := it.RawKey()
			_, _, fp := base.DecodeZsetIndexKey(keyKind, indexKey, it.RawValue())
			dataKeyLen := base.EncodeZsetDataKey(dataKey[:], keyVersion, khash, fp.Merge(), isZsetOld)
			dataWb.Delete(dataKey[:dataKeyLen])
			indexWb.Delete(indexKey)
			delCnt++
		}
		index++
		if index > stopIndex {
			break
		}
	}

	if delCnt > 0 {
		if err = dataWb.Commit(); err != nil {
			return 0, err
		}
		if err = indexWb.Commit(); err != nil {
			return 0, err
		}
		if err = zo.SetMetaDataSize(mk, khash, -delCnt); err != nil {
			return 0, err
		}
	}
	return delCnt, nil
}

func (zo *ZSetObject) ZRemRangeByScore(
	key []byte, khash uint32, min float64, max float64, leftClose bool, rightClose bool,
) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := zo.GetMetaData(mk)
	if err != nil {
		return 0, err
	}
	defer base.PutMkvToPool(mkv)
	if !mkv.IsAlive() {
		return 0, nil
	}

	dataWb := zo.GetDataWriteBatchFromPool()
	defer zo.PutWriteBatchToPool(dataWb)
	indexWb := zo.GetIndexWriteBatchFromPool()
	defer zo.PutWriteBatchToPool(indexWb)

	var index, delCnt int64
	var dataKey [base.DataKeyZsetLength]byte
	var lowerBound [base.IndexKeyScoreLength]byte
	var upperBound [base.IndexKeyScoreUpperBoundLength]byte

	stopIndex := mkv.Size() - 1
	keyVersion := mkv.Version()
	keyKind := mkv.Kind()
	isZsetOld := mkv.IsZsetOld()
	base.EncodeZsetIndexKeyScore(lowerBound[:], keyVersion, khash, min)
	base.EncodeZsetIndexKeyScoreUpperBound(upperBound[:], keyVersion, khash, max)
	iterOpts := &bitskv.IterOptions{
		KeyHash:    khash,
		UpperBound: upperBound[:],
	}
	it := zo.DataDb.NewIteratorIndex(iterOpts)
	defer it.Close()

	for it.Seek(lowerBound[:]); it.Valid(); it.Next() {
		leftPass := false
		rightPass := false
		indexKey := it.RawKey()
		version, score, fp := base.DecodeZsetIndexKey(keyKind, indexKey, it.RawValue())
		if keyVersion != version {
			break
		}
		if (leftClose && min < score) || (!leftClose && min <= score) {
			leftPass = true
		}
		if (rightClose && score < max) || (!rightClose && score <= max) {
			rightPass = true
		}
		if leftPass && rightPass {
			dataKeyLen := base.EncodeZsetDataKey(dataKey[:], mkv.Version(), khash, fp.Merge(), isZsetOld)
			dataWb.Delete(dataKey[:dataKeyLen])
			indexWb.Delete(indexKey)
			delCnt++
		}
		if !rightPass {
			break
		}
		index++
		if index > stopIndex {
			break
		}
	}

	if delCnt > 0 {
		if err = dataWb.Commit(); err != nil {
			return 0, err
		}
		if err = indexWb.Commit(); err != nil {
			return 0, err
		}
		if err = zo.SetMetaDataSize(mk, khash, -delCnt); err != nil {
			return 0, err
		}
	}
	return delCnt, nil
}

func (zo *ZSetObject) ZRemRangeByLex(key []byte, khash uint32, min []byte, max []byte, leftClose bool, rightClose bool) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := zo.GetMetaData(mk)
	if err != nil {
		return 0, err
	}
	defer base.PutMkvToPool(mkv)
	if !mkv.IsAlive() {
		return 0, nil
	}

	var leftNotLimit, rightNotLimit bool
	if bytes.Equal([]byte{'-'}, min) {
		leftNotLimit = true
	}
	if bytes.Equal([]byte{'+'}, max) {
		rightNotLimit = true
	}

	dataWb := zo.GetDataWriteBatchFromPool()
	defer zo.PutWriteBatchToPool(dataWb)
	indexWb := zo.GetIndexWriteBatchFromPool()
	defer zo.PutWriteBatchToPool(indexWb)

	var index, delCnt int64
	var dataKey [base.DataKeyZsetLength]byte
	var lowerBound [base.DataKeyHeaderLength]byte
	var upperBound [base.IndexKeyScoreLength]byte

	stopIndex := mkv.Size() - 1
	keyVersion := mkv.Version()
	keyKind := mkv.Kind()
	isZsetOld := mkv.IsZsetOld()
	base.EncodeDataKeyLowerBound(lowerBound[:], keyVersion, khash)
	base.EncodeZsetIndexKeyUpperBound(upperBound[:], keyVersion, khash)
	iterOpts := &bitskv.IterOptions{
		KeyHash:    khash,
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
	}
	it := zo.DataDb.NewIteratorIndex(iterOpts)
	defer it.Close()

	for it.Seek(lowerBound[:]); it.Valid(); it.Next() {
		leftPass := false
		rightPass := false
		indexKey := it.RawKey()
		version, _, fp := base.DecodeZsetIndexKey(keyKind, indexKey, it.RawValue())
		if keyVersion != version {
			break
		}
		member := fp.Merge()
		if leftNotLimit ||
			(leftClose && bytes.Compare(min, member) < 0) ||
			(!leftClose && bytes.Compare(min, member) <= 0) {
			leftPass = true
		}
		if rightNotLimit ||
			(rightClose && bytes.Compare(max, member) > 0) ||
			(!rightClose && bytes.Compare(max, member) >= 0) {
			rightPass = true
		}
		if leftPass && rightPass {
			dataKeyLen := base.EncodeZsetDataKey(dataKey[:], keyVersion, khash, member, isZsetOld)
			dataWb.Delete(dataKey[:dataKeyLen])
			indexWb.Delete(indexKey)
			delCnt++
		}
		if !rightPass {
			break
		}
		index++
		if index > stopIndex {
			break
		}
	}

	if delCnt > 0 {
		if err = dataWb.Commit(); err != nil {
			return 0, err
		}
		if err = indexWb.Commit(); err != nil {
			return 0, err
		}
		if err = zo.SetMetaDataSize(mk, khash, -delCnt); err != nil {
			return 0, err
		}
	}
	return delCnt, nil
}
