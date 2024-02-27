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
	"math"
	"sync"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"

	"github.com/zuoyebang/bitalostored/butils/extend"
)

const (
	MinIndex         uint32 = 0
	MaxIndex         uint32 = math.MaxUint32
	InitalLeftIndex  uint32 = math.MaxUint32 / 2
	InitalRightIndex        = InitalLeftIndex + 1
)

var mkvPool sync.Pool

func init() {
	mkvPool = sync.Pool{
		New: func() interface{} {
			return NewMetaData()
		},
	}
	for i := 0; i < 128; i++ {
		mkvPool.Put(NewMetaData())
	}
}

func GetMkvFromPool() *MetaData {
	return mkvPool.Get().(*MetaData)
}

func PutMkvToPool(mkv *MetaData) {
	mkv.Clear()
	mkvPool.Put(mkv)
}

type MetaData struct {
	dt         btools.DataType
	size       uint32
	version    uint64
	kind       uint8
	timestamp  uint64
	leftindex  uint32
	rightindex uint32
	value      []byte
}

func NewMetaData() *MetaData {
	return &MetaData{
		leftindex:  InitalLeftIndex,
		rightindex: InitalRightIndex,
	}
}

func (mkv *MetaData) initVersion(keyId uint64) {
	mkv.version = keyId
}

func (mkv *MetaData) isExistsTTL() (bool, int64) {
	if mkv.dt == btools.NoneType {
		return false, ErrnoKeyNotFoundOrExpire
	}

	if mkv.dt > btools.STRING && (mkv.version == 0 || mkv.size == 0) {
		return false, ErrnoKeyNotFoundOrExpire
	}

	if mkv.timestamp > 0 {
		nowtime := tclock.GetTimestampMilli()
		ttl := int64(mkv.timestamp)
		if ttl <= nowtime {
			return false, ErrnoKeyNotFoundOrExpire
		} else {
			return true, ttl - nowtime
		}
	}

	return true, ErrnoKeyPersist
}

func (mkv *MetaData) Reset(version uint64) {
	if version > 0 {
		var kind uint8
		if btools.IsDataTypeFieldCompress(mkv.dt) {
			kind = KeyKindFieldCompress
			mkv.version = EncodeKeyVersion(version, kind)
		} else {
			kind = KeyKindDefault
			mkv.version = version
		}
		mkv.kind = kind
	}
	mkv.size = 0
	mkv.timestamp = 0
	mkv.leftindex = InitalLeftIndex
	mkv.rightindex = InitalRightIndex
	mkv.value = nil
}

func (mkv *MetaData) Reuse(dt btools.DataType, version uint64) {
	mkv.dt = dt
	mkv.Reset(version)
}

func (mkv *MetaData) IncrSize(delta uint32) {
	mkv.size = mkv.size + delta
}

func (mkv *MetaData) SetTimestamp(timestamp uint64) {
	mkv.timestamp = timestamp
}

func (mkv *MetaData) Persist() {
	mkv.timestamp = 0
}

func (mkv *MetaData) Del() {
	mkv.timestamp = uint64(tclock.GetTimestampMilli() - 86400)
}

func (mkv *MetaData) Size() int64 {
	return int64(mkv.size)
}

func (mkv *MetaData) Version() uint64 {
	return mkv.version
}

func (mkv *MetaData) Kind() uint8 {
	return mkv.kind
}

func (mkv *MetaData) Timestamp() uint64 {
	return mkv.timestamp
}

func (mkv *MetaData) GetDataType() btools.DataType {
	return mkv.dt
}

func (mkv *MetaData) SetDataType(dt btools.DataType) {
	mkv.dt = dt
}

func (mkv *MetaData) DecrSize(delat uint32) {
	if delat > mkv.size {
		mkv.size = 0
	} else {
		mkv.size = mkv.size - delat
	}
}

func (mkv *MetaData) IsAlive() bool {
	exist, _ := mkv.isExistsTTL()
	return exist
}

func (mkv *MetaData) CheckTTL() int64 {
	_, ttl := mkv.isExistsTTL()
	return ttl
}

func (mkv *MetaData) Clear() {
	mkv.dt = 0
	mkv.size = 0
	mkv.version = 0
	mkv.kind = KeyKindDefault
	mkv.timestamp = 0
	mkv.leftindex = InitalLeftIndex
	mkv.rightindex = InitalRightIndex
	mkv.value = nil
}

func (mkv *MetaData) checkAndResetLeftRightIndex() {
	if mkv.size == 0 {
		mkv.leftindex = InitalLeftIndex
		mkv.rightindex = InitalRightIndex
	}
}

func (mkv *MetaData) SetLeftIndex(index uint32) {
	mkv.leftindex = index
}

func (mkv *MetaData) SetRightIndex(index uint32) {
	mkv.rightindex = index
}

func (mkv *MetaData) GetLeftIndex() uint32 {
	return mkv.leftindex
}

func (mkv *MetaData) GetRightIndex() uint32 {
	return mkv.rightindex
}

func (mkv *MetaData) GetLeftElementIndex() uint32 {
	return mkv.leftindex + 1
}

func (mkv *MetaData) GetRightElementIndex() uint32 {
	return mkv.rightindex - 1
}

func (mkv *MetaData) GetLeftElementIndexByte() []byte {
	return extend.Uint32ToBytes(mkv.leftindex + 1)
}

func (mkv *MetaData) GetRightElementIndexByte() []byte {
	return extend.Uint32ToBytes(mkv.rightindex - 1)
}

func (mkv *MetaData) GetLeftIndexByte() []byte {
	return extend.Uint32ToBytes(mkv.leftindex)
}

func (mkv *MetaData) GetRightIndexByte() []byte {
	return extend.Uint32ToBytes(mkv.rightindex)
}

func (mkv *MetaData) ModifyRightIndex(delta int32) {
	if delta < 0 {
		mkv.rightindex -= uint32(-delta)
	} else {
		mkv.rightindex += uint32(delta)
	}
}

func (mkv *MetaData) ModifyLeftIndex(delta int32) {
	if delta < 0 {
		mkv.leftindex += uint32(-delta)
	} else {
		mkv.leftindex -= uint32(delta)
	}
}
