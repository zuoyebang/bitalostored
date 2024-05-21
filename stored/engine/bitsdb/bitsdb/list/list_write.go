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
	"math"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
)

func (lo *ListObject) LPop(key []byte, khash uint32) ([]byte, func(), error) {
	return lo.pop(key, khash, true)
}

func (lo *ListObject) RPop(key []byte, khash uint32) ([]byte, func(), error) {
	return lo.pop(key, khash, false)
}

func (lo *ListObject) LTrim(key []byte, khash uint32, start, stop int64) error {
	return lo.ltrim2(key, khash, start, stop)
}

func (lo *ListObject) LTrimFront(key []byte, khash uint32, trimSize int64) (int64, error) {
	return lo.ltrim(key, khash, trimSize, true)
}

func (lo *ListObject) LTrimBack(key []byte, khash uint32, trimSize int64) (int64, error) {
	return lo.ltrim(key, khash, trimSize, false)
}

func (lo *ListObject) LPush(key []byte, khash uint32, args ...[]byte) (int64, error) {
	return lo.ListPush(key, khash, true, false, args...)
}

func (lo *ListObject) RPush(key []byte, khash uint32, args ...[]byte) (int64, error) {
	return lo.ListPush(key, khash, false, false, args...)
}

func (lo *ListObject) LPushX(key []byte, khash uint32, args ...[]byte) (int64, error) {
	return lo.ListPush(key, khash, true, true, args...)
}

func (lo *ListObject) RPushX(key []byte, khash uint32, args ...[]byte) (int64, error) {
	return lo.ListPush(key, khash, false, true, args...)
}

func (lo *ListObject) LSet(key []byte, khash uint32, index int64, value []byte) error {
	if len(key) > btools.MaxKeySize {
		return errn.ErrKeySize
	}
	if index > int64(math.MaxUint32) {
		return errIndexOverflow
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
	if !mkv.IsAlive() || mkv.Size() <= 0 {
		return ErrNoSuchKey
	}

	if index >= mkv.Size() || -index > mkv.Size() {
		return ErrIndexOutOfRange
	}

	var seq int64
	if index >= 0 {
		lindex := mkv.GetLeftElementIndex()
		seq = int64(lindex) + index
	} else {
		rindex := mkv.GetRightElementIndex()
		seq = int64(rindex) + index + 1
	}
	wb := lo.GetDataWriteBatchFromPool()
	defer lo.PutWriteBatchToPool(wb)

	readIndexBuf := extend.Uint32ToBytes(uint32(seq))
	ekf, ekfCloser := base.EncodeDataKey(mkv.Version(), khash, readIndexBuf)
	defer ekfCloser()
	_ = wb.Put(ekf, value)
	return wb.Commit()
}

func (lo *ListObject) LInsert(key []byte, khash uint32, isbefore bool, pivot, value []byte) (int64, error) {
	return lo.linsert(key, khash, isbefore, pivot, value)
}

func (lo *ListObject) LRem(key []byte, khash uint32, count int64, value []byte) (int64, error) {
	return lo.lrem(key, khash, count, value)
}
