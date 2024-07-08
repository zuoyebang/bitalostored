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
	"errors"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/bytepools"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

const (
	keySlotIdLength    int = 2
	keyDataTypeLength  int = 1
	keySizeLength      int = 4
	keyVersionLength   int = 8
	keyTimestampLength int = 8
	MaxFieldLength     int = 8
	ScoreLength        int = 8
	FieldMd5Length     int = 16

	expireKeyHeaderLength       = keyTimestampLength + keyDataTypeLength + keyVersionLength
	expireKeyStringHeaderLength = keyTimestampLength + keyDataTypeLength

	MetaListPosIndex   = 4
	MetaStringValueLen = keyDataTypeLength + keyTimestampLength
	MetaMixValueLen    = MetaStringValueLen + keySizeLength + keyVersionLength
	MetaListValueLen   = MetaMixValueLen + MetaListPosIndex*2

	DataKeyHeaderLength     = keySlotIdLength + keyVersionLength
	DataKeyZsetLength       = DataKeyHeaderLength + FieldMd5Length
	DataKeyZsetOldLength    = keySlotIdLength + FieldMd5Length
	DataKeyListIndex        = DataKeyHeaderLength + 4
	DataKeyUpperBoundLength = DataKeyHeaderLength + MaxFieldLength

	IndexKeyScoreLength           = DataKeyHeaderLength + ScoreLength
	IndexKeyScoreUpperBoundLength = IndexKeyScoreLength + MaxFieldLength
)

const (
	ErrnoKeyNotFoundOrExpire = -2
	ErrnoKeyPersist          = -1
)

var (
	MaxUpperBound = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	NilDataVal    = []byte{0}
)

var (
	errEncodeKVKey    = errors.New("invalid encode kv key")
	errFieldEncodeKey = errors.New("invalid encode field key")
	errMetaDataKeyLen = errors.New("invalid metadb val len")
)

func CheckMetaKey(k []byte) (int, error) {
	if len(k) <= keySlotIdLength {
		return 0, errn.ErrKeySize
	}
	return keySlotIdLength, nil
}

func EncodeMetaKey(key []byte, khash uint32) ([]byte, func()) {
	size := keySlotIdLength + len(key)
	pool, closer := bytepools.BytePools.GetBytePool(size)
	binary.LittleEndian.PutUint16(pool[:keySlotIdLength], utils.GetSlotId(khash))
	copy(pool[keySlotIdLength:size], key)
	return pool[:size], closer
}

func EncodeMetaKeyForLua(key []byte) ([]byte, func()) {
	size := keySlotIdLength + len(key)
	pool, closer := bytepools.BytePools.GetBytePool(size)
	binary.LittleEndian.PutUint16(pool[:keySlotIdLength], btools.LuaScriptSlot)
	copy(pool[keySlotIdLength:size], key)
	return pool[:size], closer
}

func DecodeMetaKey(ek []byte) ([]byte, error) {
	pos, err := CheckMetaKey(ek)
	if err != nil {
		return nil, err
	}
	return ek[pos:], nil
}

func EncodeMetaDbValueForString(buf []byte, timestamp uint64) {
	buf[0] = uint8(btools.STRING)
	binary.BigEndian.PutUint64(buf[1:], timestamp)
}

func EncodeMetaDbValueForMix(buf []byte, mkv *MetaData) {
	buf[0] = uint8(mkv.dt)
	pos := 1
	binary.BigEndian.PutUint32(buf[pos:], mkv.size)
	pos += keySizeLength
	binary.BigEndian.PutUint64(buf[pos:], mkv.version)
	pos += keyVersionLength
	binary.BigEndian.PutUint64(buf[pos:], mkv.timestamp)
}

func EncodeMetaDbValueForList(buf []byte, mkv *MetaData) {
	mkv.checkAndResetLeftRightIndex()

	buf[0] = uint8(mkv.dt)
	pos := 1
	binary.BigEndian.PutUint32(buf[pos:], mkv.size)
	pos += keySizeLength
	binary.BigEndian.PutUint64(buf[pos:], mkv.version)
	pos += keyVersionLength
	binary.BigEndian.PutUint64(buf[pos:], mkv.timestamp)
	pos += keyTimestampLength
	binary.BigEndian.PutUint32(buf[pos:], mkv.leftindex)
	pos += MetaListPosIndex
	binary.BigEndian.PutUint32(buf[pos:], mkv.rightindex)
}

func DecodeMetaValue(mkv *MetaData, val []byte) error {
	if len(val) < MetaStringValueLen {
		return errMetaDataKeyLen
	}

	mkv.dt = btools.DataType(val[0])
	switch mkv.dt {
	case btools.STRING:
		_, mkv.timestamp, mkv.value = DecodeMetaValueForString(val)
		return nil
	case btools.LIST:
		return DecodeMetaValueForList(mkv, val)
	default:
		return DecodeMetaValueForMix(mkv, val)
	}
}

func DecodeMetaValueForString(eval []byte) (dt btools.DataType, timestamp uint64, val []byte) {
	evalLen := len(eval)
	if evalLen < MetaStringValueLen {
		return btools.NoneType, 0, nil
	}

	dt = btools.DataType(eval[0])
	pos := 1
	timestamp = binary.BigEndian.Uint64(eval[pos:])
	pos += keyTimestampLength
	val = eval[pos:evalLen]
	return dt, timestamp, val
}

func DecodeMetaValueForMix(mkv *MetaData, val []byte) error {
	if len(val) < MetaMixValueLen {
		return errMetaDataKeyLen
	}

	pos := 1
	mkv.size = binary.BigEndian.Uint32(val[pos:])
	pos += keySizeLength
	mkv.version = binary.BigEndian.Uint64(val[pos:])
	mkv.kind = DecodeKeyVersionKind(mkv.version)
	pos += keyVersionLength
	mkv.timestamp = binary.BigEndian.Uint64(val[pos:])
	return nil
}

func DecodeMetaValueForList(mkv *MetaData, val []byte) error {
	if len(val) < MetaListValueLen {
		return errMetaDataKeyLen
	}

	pos := 1
	mkv.size = binary.BigEndian.Uint32(val[pos:])
	pos += keySizeLength
	mkv.version = binary.BigEndian.Uint64(val[pos:])
	mkv.kind = DecodeKeyVersionKind(mkv.version)
	pos += keyVersionLength
	mkv.timestamp = binary.BigEndian.Uint64(val[pos:])
	pos += keyTimestampLength
	mkv.leftindex = binary.BigEndian.Uint32(val[pos:])
	pos += MetaListPosIndex
	mkv.rightindex = binary.BigEndian.Uint32(val[pos:])
	return nil
}
