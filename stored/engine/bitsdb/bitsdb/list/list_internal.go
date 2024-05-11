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

package list

import (
	"bytes"
	"container/list"
	"context"
	"encoding/binary"
	"sync"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
)

func (lo *ListObject) ListPush(key []byte, khash uint32, isleft bool, checkExist bool, args ...[]byte) (int64, error) {
	if len(key) > btools.MaxKeySize {
		return 0, errn.ErrKeySize
	}

	unlockKey := lo.LockKey(khash)
	defer unlockKey()

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := lo.GetMetaDataNoneType(mk)
	if err != nil {
		return 0, err
	}
	defer base.PutMkvToPool(mkv)

	isAlive, err := lo.CheckMetaData(mkv)
	if err != nil || (!isAlive && checkExist) {
		return 0, err
	}

	if !lo.isEnoughSpace(mkv, uint32(len(args))) {
		return 0, ErrWriteNoSpace
	}

	wb := lo.GetDataWriteBatchFromPool()
	defer lo.PutWriteBatchToPool(wb)

	var index []byte
	keyVersion := mkv.Version()
	for i := range args {
		if isleft {
			index = mkv.GetLeftIndexByte()
		} else {
			index = mkv.GetRightIndexByte()
		}

		ekf, ekfCloser := base.EncodeDataKey(keyVersion, khash, index)
		_ = wb.Put(ekf, args[i])
		ekfCloser()

		if isleft {
			mkv.ModifyLeftIndex(1)
		} else {
			mkv.ModifyRightIndex(1)
		}
		mkv.IncrSize(1)
	}

	if err = wb.Commit(); err != nil {
		return 0, err
	}

	if err = lo.SetMetaData(mk, mkv); err != nil {
		return 0, err
	}

	return mkv.Size(), nil
}

func (lo *ListObject) pop(key []byte, khash uint32, isleft bool) ([]byte, func(), error) {
	if len(key) > btools.MaxKeySize {
		return nil, nil, errn.ErrKeySize
	}

	unlockKey := lo.LockKey(khash)
	defer unlockKey()

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := lo.GetMetaData(mk)
	if err != nil {
		return nil, nil, err
	}
	defer base.PutMkvToPool(mkv)
	if !mkv.IsAlive() || mkv.Size() <= 0 {
		return nil, nil, err
	}

	var value, ekf, indexBuf []byte
	var exist bool
	var ekfCloser, vcloser func()
	keyVersion := mkv.Version()
	for {
		if mkv.Size() <= 0 || mkv.GetLeftIndex() == mkv.GetRightIndex()-1 {
			break
		}

		if isleft {
			indexBuf = mkv.GetLeftElementIndexByte()
		} else {
			indexBuf = mkv.GetRightElementIndexByte()
		}

		ekf, ekfCloser = base.EncodeDataKey(keyVersion, khash, indexBuf)
		value, exist, vcloser, err = lo.GetDataValue(ekf)
		ekfCloser()

		if isleft {
			mkv.ModifyLeftIndex(-1)
		} else {
			mkv.ModifyRightIndex(-1)
		}
		mkv.DecrSize(1)

		if exist {
			break
		}

		if vcloser != nil {
			vcloser()
		}
	}

	if !exist {
		mkv.Reset(lo.GetNextKeyId())
	} else {
		wb := lo.GetDataWriteBatchFromPool()
		defer lo.PutWriteBatchToPool(wb)
		_ = wb.Delete(ekf)
		if err = wb.Commit(); err != nil {
			return nil, vcloser, err
		}
	}

	if err = lo.SetMetaData(mk, mkv); err != nil {
		return nil, vcloser, err
	}
	return value, vcloser, nil
}

func (lo *ListObject) isEnoughSpace(mkv *base.MetaData, n uint32) bool {
	leftIndex := mkv.GetLeftIndex()
	rightIndex := mkv.GetRightIndex()
	if leftIndex-rightIndex <= n {
		return false
	}
	return true
}

func (lo *ListObject) linsert(key []byte, khash uint32, isbefore bool, pivot, value []byte) (int64, error) {
	if len(key) > btools.MaxKeySize {
		return 0, errn.ErrKeySize
	}

	unlockKey := lo.LockKey(khash)
	defer unlockKey()

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := lo.GetMetaData(mk)
	if err != nil {
		return 0, err
	}
	defer base.PutMkvToPool(mkv)
	if !mkv.IsAlive() {
		return 0, nil
	}
	if mkv.Size() > ListCopyMax {
		return 0, errMoveTooMany
	}

	if !lo.isEnoughSpace(mkv, 1) {
		return 0, ErrWriteNoSpace
	}

	var it *bitskv.Iterator
	keyVersion := mkv.Version()
	ekf, ekfCloser := base.EncodeDataKey(keyVersion, khash, mkv.GetLeftElementIndexByte())
	defer ekfCloser()
	leftStartIndex := mkv.GetLeftElementIndex()
	rightStopIndex := mkv.GetRightElementIndex()
	currentIndex := leftStartIndex
	pivotIndex := uint32(0)
	find := false

	iters := lo.getIter(khash, mkv)
	defer func() {
		for i := range iters {
			iters[i].Close()
		}
	}()

	if len(iters) == 1 {
		it = iters[0]
		for it.Seek(ekf); currentIndex <= rightStopIndex && it.Valid(); it.Next() {
			if bytes.Equal(it.Value(), pivot) {
				find = true
				pivotIndex = currentIndex
				break
			}
			currentIndex++
		}
	} else {
		it = iters[0]
		currentIndex = leftStartIndex
		for it.Seek(ekf); currentIndex <= base.MaxIndex && it.Valid(); it.Next() {
			if bytes.Equal(it.RawValue(), pivot) {
				find = true
				pivotIndex = currentIndex
				break
			}
			currentIndex++
		}

		if !find {
			it = iters[1]
			currentIndex = base.MinIndex
			ekf, ekfCloser = base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(currentIndex))
			for it.Seek(ekf); currentIndex <= rightStopIndex && it.Valid(); it.Next() {
				if bytes.Equal(it.RawValue(), pivot) {
					find = true
					pivotIndex = currentIndex
					break
				}
				currentIndex++
			}
			ekfCloser()
		}
	}

	if !find {
		return -1, nil
	}

	wb := lo.GetDataWriteBatchFromPool()
	defer lo.PutWriteBatchToPool(wb)

	targetIndex := uint32(0)
	listNode := make([][]byte, 0)

	var pivotLeftDistance, pivotRightDistance uint32
	if len(iters) == 1 {
		pivotLeftDistance = pivotIndex - leftStartIndex
		pivotRightDistance = rightStopIndex - pivotIndex
	} else {
		if pivotIndex > leftStartIndex {
			pivotLeftDistance = pivotIndex - leftStartIndex
			pivotRightDistance = base.MaxIndex - pivotIndex + rightStopIndex
		} else {
			pivotLeftDistance = base.MaxIndex - leftStartIndex + pivotIndex
			pivotRightDistance = rightStopIndex - pivotIndex
		}
	}

	moveLeft := func() {
		if isbefore {
			targetIndex = pivotIndex - 1
		} else {
			targetIndex = pivotIndex
		}
		currentIndex = leftStartIndex

		ekfLeft1, ekfLeftCloser1 := base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(currentIndex))
		for it.Seek(ekfLeft1); it.Valid() && currentIndex <= pivotIndex; it.Next() {
			if currentIndex == pivotIndex {
				if !isbefore {
					listNode = append(listNode, it.Value())
				}
				break
			}
			listNode = append(listNode, it.Value())
			currentIndex++
		}
		ekfLeftCloser1()

		currentIndex = leftStartIndex - 1
		for _, node := range listNode {
			ekfLeft2, ekfLeftCloser2 := base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(currentIndex))
			_ = wb.Put(ekfLeft2, node)
			ekfLeftCloser2()
			currentIndex++
		}
		mkv.ModifyLeftIndex(1)
	}
	moveRight := func() {
		if isbefore {
			targetIndex = pivotIndex
		} else {
			targetIndex = pivotIndex + 1
		}
		currentIndex = pivotIndex

		ekfRight1, ekfRight1Closer := base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(currentIndex))
		for it.Seek(ekfRight1); it.Valid() && currentIndex <= rightStopIndex; it.Next() {
			if currentIndex == pivotIndex && !isbefore {
				currentIndex++
				continue
			}
			listNode = append(listNode, it.Value())
			currentIndex++
		}
		ekfRight1Closer()

		currentIndex = targetIndex + 1
		for _, node := range listNode {
			ekfRight2, ekfRight2Closer := base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(currentIndex))
			_ = wb.Put(ekfRight2, node)
			ekfRight2Closer()
			currentIndex++
		}
		mkv.ModifyRightIndex(1)
	}

	if len(iters) == 1 {
		it = iters[0]
		if pivotLeftDistance <= pivotRightDistance {
			moveLeft()
		} else {
			moveRight()
		}
	} else {
		if pivotIndex > rightStopIndex {
			it = iters[0]
			moveLeft()
		} else {
			it = iters[1]
			moveRight()
		}
	}
	mkv.IncrSize(1)
	ekfNew, ekfNewCloser := base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(targetIndex))
	_ = wb.Put(ekfNew, value)
	ekfNewCloser()
	if err = wb.Commit(); err != nil {
		return 0, err
	}

	if err = lo.SetMetaData(mk, mkv); err != nil {
		return 0, err
	}

	return mkv.Size(), nil
}

func (lo *ListObject) lrem(key []byte, khash uint32, count int64, value []byte) (int64, error) {
	if len(key) > btools.MaxKeySize {
		return 0, errn.ErrKeySize
	}

	unlockKey := lo.LockKey(khash)
	defer unlockKey()

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := lo.GetMetaData(mk)
	if err != nil {
		return 0, err
	}
	defer base.PutMkvToPool(mkv)
	if !mkv.IsAlive() {
		return 0, nil
	}

	needDelCnt := count
	if count < 0 {
		needDelCnt = -count
	}

	leftStartIndex := mkv.GetLeftElementIndex()
	rightStopIndex := mkv.GetRightElementIndex()
	equalIndex := make([]uint32, 0, needDelCnt)
	iters := lo.getIter(khash, mkv)
	defer func() {
		for i := range iters {
			iters[i].Close()
		}
	}()

	var deleteIndex []uint32
	var it *bitskv.Iterator
	wb := lo.GetDataWriteBatchFromPool()
	defer lo.PutWriteBatchToPool(wb)
	keyVersion := mkv.Version()

	if len(iters) == 1 {
		it = iters[0]
		if count >= 0 {
			idx := leftStartIndex
			for it.First(); it.Valid() && idx <= rightStopIndex; it.Next() {
				if count == 0 {
					if bytes.Equal(it.RawValue(), value) {
						equalIndex = append(equalIndex, idx)
					}
				} else {
					if bytes.Equal(it.RawValue(), value) {
						equalIndex = append(equalIndex, idx)
						needDelCnt--
						if needDelCnt == 0 {
							break
						}
					}
				}
				idx++
				if idx > rightStopIndex {
					break
				}
			}
		} else {
			efk, efkCloser := base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(rightStopIndex))
			defer efkCloser()
			idx := rightStopIndex
			for it.Seek(efk); it.Valid() && needDelCnt > 0 && idx >= leftStartIndex; it.Prev() {
				if bytes.Equal(it.RawValue(), value) {
					if f, _, err := base.DecodeDataKey(it.RawKey()); err == nil {
						equalIndex = append(equalIndex, binary.BigEndian.Uint32(f))
						needDelCnt--
					}
				}
				if idx == leftStartIndex {
					break
				}
				idx--
			}
		}

		if len(equalIndex) <= 0 {
			return 0, nil
		}

		deleteIndex = make([]uint32, 0, len(equalIndex))
		var subLeftIndex uint32
		var subRightIndex uint32
		needDelCnt = int64(len(equalIndex))
		if count >= 0 {
			subLeftIndex = equalIndex[0]
			subRightIndex = equalIndex[needDelCnt-1]
		} else {
			subLeftIndex = equalIndex[needDelCnt-1]
			subRightIndex = equalIndex[0]
		}

		leftPartLen := subRightIndex - leftStartIndex
		rightPartLen := rightStopIndex - subLeftIndex

		if leftPartLen <= rightPartLen {
			left := subRightIndex
			currentIndex := subRightIndex
			ekf, ekfCloser := base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(subRightIndex))
			defer ekfCloser()
			for it.Seek(ekf); it.Valid() && currentIndex >= leftStartIndex; it.Prev() {
				if bytes.Equal(it.RawValue(), value) && needDelCnt > 0 {
					needDelCnt--
				} else {
					newEkf, newEkfCloser := base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(left))
					_ = wb.Put(newEkf, it.RawValue())
					newEkfCloser()
					left--
				}
				if currentIndex == leftStartIndex {
					break
				}
				currentIndex--
			}

			for idx := uint32(0); idx < uint32(len(equalIndex)); idx++ {
				delIndex := leftStartIndex + idx
				deleteIndex = append(deleteIndex, delIndex)
			}
			mkv.ModifyLeftIndex(int32(-len(equalIndex)))
		} else {
			right := subLeftIndex
			currentIndex := subLeftIndex
			ekf, ekfCloser := base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(subLeftIndex))
			defer ekfCloser()
			for it.Seek(ekf); it.Valid() && currentIndex <= rightStopIndex; it.Next() {
				if bytes.Equal(it.RawValue(), value) && needDelCnt > 0 {
					needDelCnt--
				} else {
					newEkf, newEkfCloser := base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(right))
					_ = wb.Put(newEkf, it.RawValue())
					newEkfCloser()
					right++
				}
				currentIndex++
				if currentIndex > rightStopIndex {
					break
				}
			}

			for idx := uint32(0); idx < uint32(len(equalIndex)); idx++ {
				delIndex := rightStopIndex - idx
				deleteIndex = append(deleteIndex, delIndex)
			}
			mkv.ModifyRightIndex(int32(-len(equalIndex)))
		}
	} else if len(iters) == 2 {
		if count >= 0 {
			func() {
				idx := leftStartIndex
				it = iters[0]
				for it.First(); it.Valid() && idx <= base.MaxIndex; it.Next() {
					if count == 0 {
						if bytes.Equal(it.RawValue(), value) {
							equalIndex = append(equalIndex, idx)
						}
					} else {
						if bytes.Equal(it.RawValue(), value) {
							equalIndex = append(equalIndex, idx)
							needDelCnt--
							if needDelCnt == 0 {
								break
							}
						}
					}
					if idx == base.MaxIndex {
						break
					}
					idx++
				}

				if count > 0 && needDelCnt == 0 {
					return
				}

				idx = base.MinIndex
				it = iters[1]
				for it.First(); it.Valid() && idx <= rightStopIndex; it.Next() {
					if count == 0 {
						if bytes.Equal(it.RawValue(), value) {
							equalIndex = append(equalIndex, idx)
						}
					} else {
						if bytes.Equal(it.RawValue(), value) {
							equalIndex = append(equalIndex, idx)
							needDelCnt--
							if needDelCnt == 0 {
								break
							}
						}
					}
					idx++
					if idx > rightStopIndex {
						break
					}
				}
			}()
		} else {
			func() {
				it = iters[1]
				idx := rightStopIndex
				efk, efkCloser := base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(rightStopIndex))
				defer efkCloser()
				for it.Seek(efk); it.Valid() && needDelCnt > 0; it.Prev() {
					if bytes.Equal(it.RawValue(), value) {
						equalIndex = append(equalIndex, idx)
						needDelCnt--
					}
					if idx == 0 {
						break
					}
					idx--
				}
				if needDelCnt == 0 {
					return
				}

				it = iters[0]
				efk, efkCloser = base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(base.MaxIndex))
				defer efkCloser()
				idx = base.MaxIndex
				for it.Seek(efk); it.Valid() && needDelCnt > 0 && idx >= leftStartIndex; it.Prev() {
					if bytes.Equal(it.RawValue(), value) {
						equalIndex = append(equalIndex, idx)
						needDelCnt--
					}
					if idx == leftStartIndex {
						break
					}
					idx--
				}
			}()
		}

		if len(equalIndex) <= 0 {
			return 0, nil
		}

		deleteIndex = make([]uint32, 0, len(equalIndex))
		var subLeftIndex uint32
		var subRightIndex uint32
		needDelCnt = int64(len(equalIndex))
		if count >= 0 {
			subLeftIndex = equalIndex[0]
			subRightIndex = equalIndex[needDelCnt-1]
		} else {
			subLeftIndex = equalIndex[needDelCnt-1]
			subRightIndex = equalIndex[0]
		}

		getLeftPartLen := func(mkv *base.MetaData, leftIndex uint32) uint32 {
			firstIndex := mkv.GetLeftIndex()
			if leftIndex >= firstIndex {
				return leftIndex - firstIndex
			} else {
				return leftIndex + base.MaxIndex - firstIndex
			}
		}
		getRightPartLen := func(mkv *base.MetaData, rightIndex uint32) uint32 {
			lastIndex := mkv.GetRightIndex()
			if rightIndex <= lastIndex {
				return lastIndex - rightIndex
			} else {
				return lastIndex + base.MaxIndex - rightIndex
			}
		}

		leftPartLen := getLeftPartLen(mkv, subLeftIndex)
		rightPartLen := getRightPartLen(mkv, subRightIndex)

		if leftPartLen <= rightPartLen {
			fillIndex := subRightIndex
			currentIndex := subRightIndex
			ekf, ekfCloser := base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(subRightIndex))
			defer ekfCloser()
			if subRightIndex < leftStartIndex {
				it = iters[1]
				for it.Seek(ekf); it.Valid() && currentIndex >= base.MinIndex; it.Prev() {
					if bytes.Equal(it.RawValue(), value) && needDelCnt > 0 {
						needDelCnt--
					} else {
						newEkf, newEkfCloser := base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(fillIndex))
						_ = wb.Put(newEkf, it.RawValue())
						newEkfCloser()
						fillIndex = fillIndex - 1
					}
					if currentIndex == base.MinIndex {
						break
					}
					currentIndex--
				}
				if currentIndex == base.MinIndex {
					currentIndex = base.MaxIndex
				}
			}

			it = iters[0]
			ekf, ekfCloser = base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(currentIndex))
			defer ekfCloser()

			for it.Seek(ekf); it.Valid() && currentIndex >= leftStartIndex; it.Prev() {
				if bytes.Equal(it.RawValue(), value) && needDelCnt > 0 {
					needDelCnt--
				} else {
					newEkf, newEkfCloser := base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(fillIndex))
					_ = wb.Put(newEkf, it.RawValue())
					newEkfCloser()
					if fillIndex == base.MinIndex {
						fillIndex = base.MaxIndex
					} else {
						fillIndex = fillIndex - 1
					}
				}
				if currentIndex == leftStartIndex {
					break
				}
				currentIndex--
			}

			for idx := uint32(0); idx < uint32(len(equalIndex)); idx++ {
				delIndex := leftStartIndex + idx
				deleteIndex = append(deleteIndex, delIndex)
			}
			mkv.ModifyLeftIndex(int32(-len(equalIndex)))
		} else {
			fillIndex := subLeftIndex
			currentIndex := subLeftIndex
			ekf, ekfCloser := base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(subLeftIndex))
			defer ekfCloser()
			if subLeftIndex > rightStopIndex {
				it = iters[0]
				for it.Seek(ekf); it.Valid() && currentIndex <= base.MaxIndex; it.Next() {
					if bytes.Equal(it.RawValue(), value) && needDelCnt > 0 {
						needDelCnt--
					} else {
						newEkf, newEkfCloser := base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(fillIndex))
						_ = wb.Put(newEkf, it.RawValue())
						newEkfCloser()
						fillIndex = fillIndex + 1
					}
					if currentIndex == base.MaxIndex {
						break
					}
					currentIndex++
				}
				if currentIndex == base.MaxIndex {
					currentIndex = base.MinIndex
				}
			}
			it = iters[1]
			ekf, ekfCloser = base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(currentIndex))
			defer ekfCloser()

			for it.Seek(ekf); it.Valid() && currentIndex <= rightStopIndex; it.Next() {
				if bytes.Equal(it.RawValue(), value) && needDelCnt > 0 {
					needDelCnt--
				} else {
					newEkf, newEkfCloser := base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(fillIndex))
					_ = wb.Put(newEkf, it.RawValue())
					newEkfCloser()
					if fillIndex == base.MaxIndex {
						fillIndex = base.MinIndex
					} else {
						fillIndex = fillIndex + 1
					}
				}
				currentIndex++
				if currentIndex > rightStopIndex {
					break
				}
			}

			for idx := uint32(0); idx < uint32(len(equalIndex)); idx++ {
				delIndex := rightStopIndex - idx
				deleteIndex = append(deleteIndex, delIndex)
			}
			mkv.ModifyRightIndex(int32(-len(equalIndex)))
		}
	}

	mkv.DecrSize(uint32(len(deleteIndex)))

	for _, dIndex := range deleteIndex {
		ekf, ekfCloser := base.EncodeDataKey(keyVersion, khash, extend.Uint32ToBytes(dIndex))
		_ = wb.Delete(ekf)
		ekfCloser()
	}

	if err = wb.Commit(); err != nil {
		return 0, err
	}

	if err = lo.SetMetaData(mk, mkv); err != nil {
		return 0, err
	}

	return int64(len(deleteIndex)), nil
}

func (lo *ListObject) ltrim2(key []byte, khash uint32, start, stop int64) (err error) {
	if len(key) > btools.MaxKeySize {
		return errn.ErrKeySize
	}

	unlockKey := lo.LockKey(khash)
	defer unlockKey()

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := lo.GetMetaData(mk)
	if err != nil {
		return err
	}
	defer base.PutMkvToPool(mkv)
	if !mkv.IsAlive() {
		return nil
	}

	llen := mkv.Size()
	if start < 0 {
		start = llen + start
	}
	if stop < 0 {
		stop = llen + stop
	}

	if start >= llen || start > stop {
		mkv.Reset(0)
		return lo.SetMetaData(mk, mkv)
	}

	if start < 0 {
		start = 0
	}
	if stop >= llen {
		stop = llen - 1
	}

	wb := lo.GetDataWriteBatchFromPool()
	defer lo.PutWriteBatchToPool(wb)
	keyVersion := mkv.Version()
	if start > 0 {
		for i := int64(0); i < start; i++ {
			lindex := mkv.GetLeftElementIndexByte()
			ekf, ekfCloser := base.EncodeDataKey(keyVersion, khash, lindex)
			_ = wb.Delete(ekf)
			ekfCloser()

			mkv.ModifyLeftIndex(-1)
			mkv.DecrSize(1)
		}
	}
	if stop < llen-1 {
		for i := stop + 1; i < llen; i++ {
			rindex := mkv.GetRightElementIndexByte()
			ekf, ekfCloser := base.EncodeDataKey(keyVersion, khash, rindex)
			_ = wb.Delete(ekf)
			ekfCloser()

			mkv.ModifyRightIndex(-1)
			mkv.DecrSize(1)
		}
	}

	if err = wb.Commit(); err != nil {
		return err
	}

	if err = lo.SetMetaData(mk, mkv); err != nil {
		return err
	}

	return nil
}

func (lo *ListObject) ltrim(key []byte, khash uint32, trimSize int64, isleft bool) (int64, error) {
	if len(key) > btools.MaxKeySize {
		return 0, errn.ErrKeySize
	}
	if trimSize <= 0 {
		return 0, nil
	}

	unlockKey := lo.LockKey(khash)
	defer unlockKey()

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := lo.GetMetaData(mk)
	if err != nil {
		return 0, err
	}
	defer base.PutMkvToPool(mkv)
	if !mkv.IsAlive() {
		return 0, nil
	}

	listSize := mkv.Size()
	if listSize <= trimSize {
		mkv.Reset(0)
		if err = lo.SetMetaData(mk, mkv); err != nil {
			return 0, err
		}
		return listSize, nil
	}

	wb := lo.GetDataWriteBatchFromPool()
	defer lo.PutWriteBatchToPool(wb)
	keyVersion := mkv.Version()
	if isleft {
		for i := int64(0); i < trimSize; i++ {
			lindex := mkv.GetLeftElementIndexByte()
			ekf, ekfCloser := base.EncodeDataKey(keyVersion, khash, lindex)
			_ = wb.Delete(ekf)
			ekfCloser()

			mkv.ModifyLeftIndex(-1)
			mkv.DecrSize(1)
		}
	} else {
		for i := int64(0); i < trimSize; i++ {
			rindex := mkv.GetRightElementIndexByte()
			ekf, ekfCloser := base.EncodeDataKey(keyVersion, khash, rindex)
			_ = wb.Delete(ekf)
			ekfCloser()

			mkv.ModifyRightIndex(-1)
			mkv.DecrSize(1)
		}
	}
	if err = wb.Commit(); err != nil {
		return 0, err
	}

	if err = lo.SetMetaData(mk, mkv); err != nil {
		return 0, err
	}

	return trimSize, nil
}

func (lo *ListObject) lSignalAsReady(key []byte) {
	lo.lbkeys.signal(key)
}

func (lo *ListObject) getIter(khash uint32, mkv *base.MetaData) []*bitskv.Iterator {
	iters := make([]*bitskv.Iterator, 0, 1)

	leftIndex := mkv.GetLeftElementIndex()
	rightIndex := mkv.GetRightIndex()
	keyVersion := mkv.Version()
	it1Opts := &bitskv.IterOptions{
		LowerBound: base.EncodeListDataKey(keyVersion, khash, leftIndex),
		UpperBound: base.EncodeListDataKeyUpperBound(keyVersion, khash),
		KeyHash:    khash,
	}
	it1 := lo.DataDb.NewIterator(it1Opts)
	iters = append(iters, it1)

	if mkv.GetLeftIndex() >= rightIndex {
		leftIndex = base.MinIndex
		it2Opts := &bitskv.IterOptions{
			LowerBound: base.EncodeListDataKey(keyVersion, khash, leftIndex),
			UpperBound: base.EncodeListDataKey(keyVersion, khash, rightIndex),
			KeyHash:    khash,
		}
		it2 := lo.DataDb.NewIterator(it2Opts)
		iters = append(iters, it2)
	}

	return iters
}

type lBlockKeys struct {
	sync.Mutex

	keys map[string]*list.List
}

func newLBlockKeys() *lBlockKeys {
	l := new(lBlockKeys)

	l.keys = make(map[string]*list.List)
	return l
}

func (l *lBlockKeys) signal(key []byte) {
	l.Lock()
	defer l.Unlock()

	s := unsafe2.String(key)
	fns, ok := l.keys[s]
	if !ok {
		return
	}
	for e := fns.Front(); e != nil; e = e.Next() {
		fn := e.Value.(context.CancelFunc)
		fn()
	}

	delete(l.keys, s)
}
