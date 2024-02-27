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
	"bytes"
	"crypto/md5"
	"encoding/binary"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/bytepools"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

const (
	KeyFieldCompressSize   = 512
	KeyFieldCompressPrefix = KeyFieldCompressSize >> 1
)

const (
	DataValueKindDefault uint8 = iota
	DataValueKindFieldCompress
)

const (
	DataValueKindOffset  = 0
	DataValueFieldOffset = 1
)

func GetDataValueKind(v []byte) uint8 {
	if len(v) == 0 || bytes.Equal(v, NilDataVal) {
		return DataValueKindDefault
	}
	return v[DataValueKindOffset]
}

func IsDataValueFieldCompress(kind uint8, v []byte) bool {
	if kind == KeyKindFieldCompress && GetDataValueKind(v) == DataValueKindFieldCompress {
		return true
	}
	return false
}

func PutDataKeyHeader(buf []byte, version uint64, khash uint32) {
	binary.LittleEndian.PutUint16(buf[0:keySlotIdLength], utils.GetSlotId(khash))
	binary.LittleEndian.PutUint64(buf[keySlotIdLength:DataKeyHeaderLength], version)
}

func EncodeDataKeyLowerBound(buf []byte, version uint64, khash uint32) {
	PutDataKeyHeader(buf, version, khash)
}

func EncodeDataKeyUpperBound(buf []byte, version uint64, khash uint32) {
	PutDataKeyHeader(buf, version, khash)
	copy(buf[DataKeyHeaderLength:DataKeyUpperBoundLength], MaxUpperBound)
}

func EncodeListDataKeyUpperBound(version uint64, khash uint32) []byte {
	buf := make([]byte, DataKeyUpperBoundLength)
	EncodeDataKeyUpperBound(buf, version, khash)
	return buf
}

func EncodeListDataKey(version uint64, khash uint32, index uint32) []byte {
	buf := make([]byte, DataKeyListIndex)
	EncodeListDataKeyIndex(buf, version, khash, index)
	return buf
}

func EncodeListDataKeyIndex(buf []byte, version uint64, khash uint32, index uint32) {
	PutDataKeyHeader(buf, version, khash)
	binary.BigEndian.PutUint32(buf[DataKeyHeaderLength:], index)
}

func EncodeDataKey(version uint64, khash uint32, field []byte) ([]byte, func()) {
	size := DataKeyHeaderLength + len(field)
	buf, closer := bytepools.BytePools.GetBytePool(size)
	PutDataKeyHeader(buf, version, khash)
	if field != nil {
		copy(buf[DataKeyHeaderLength:], field)
	}

	return buf[:size], closer
}

func DecodeDataKey(key []byte) (field []byte, version uint64, err error) {
	if len(key) < DataKeyHeaderLength {
		return nil, 0, errFieldEncodeKey
	}
	pos := keySlotIdLength
	version = binary.LittleEndian.Uint64(key[pos : pos+keyVersionLength])
	pos += keyVersionLength
	field = key[pos:]
	return field, version, nil
}

func DecodeDataKeyHeader(key []byte) (version uint64, header []byte) {
	if len(key) < DataKeyHeaderLength {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(key[keySlotIdLength:DataKeyHeaderLength]), key[:DataKeyHeaderLength]
}

func EncodeSetDataKey(version uint64, kind uint8, khash uint32, field []byte) ([]byte, func(), bool) {
	fieldSize := len(field)
	isCompress := false
	if kind == KeyKindFieldCompress && fieldSize > KeyFieldCompressSize {
		isCompress = true
		fieldSize = KeyFieldCompressPrefix + FieldMd5Length
	}

	size := DataKeyHeaderLength + fieldSize
	buf, closer := bytepools.BytePools.GetBytePool(size)
	PutDataKeyHeader(buf, version, khash)

	if fieldSize > 0 {
		pos := DataKeyHeaderLength
		if isCompress {
			copy(buf[pos:pos+KeyFieldCompressPrefix], field[0:KeyFieldCompressPrefix])
			fieldMd5 := md5.Sum(field)
			copy(buf[pos+KeyFieldCompressPrefix:], fieldMd5[:])
		} else {
			copy(buf[pos:], field)
		}
	}

	return buf[:size], closer, isCompress
}

func DecodeSetDataKey(keyKind uint8, key, value []byte) (version uint64, field btools.FieldPair) {
	if len(key) < DataKeyHeaderLength {
		return
	}

	pos := keySlotIdLength
	version = binary.LittleEndian.Uint64(key[pos : pos+keyVersionLength])
	pos += keyVersionLength
	isCompress := IsDataValueFieldCompress(keyKind, value)
	if !isCompress {
		field.Prefix = key[pos:]
		field.Suffix = nil
	} else {
		field.Prefix = key[pos : pos+KeyFieldCompressPrefix]
		field.Suffix = value[DataValueFieldOffset:]
	}

	return
}

func EncodeDataKeyCursor(version uint64, khash uint32, cursor []byte) ([]byte, func()) {
	size := DataKeyHeaderLength + len(cursor)
	buf, closer := bytepools.BytePools.GetBytePool(size)
	PutDataKeyHeader(buf, version, khash)
	copy(buf[DataKeyHeaderLength:], cursor)
	return buf[:size], closer
}

func DecodeDataKeyCursor(keyKind uint8, key, value []byte) (version uint64, field btools.FieldPair, cursor []byte) {
	version, field = DecodeSetDataKey(keyKind, key, value)
	if version == 0 {
		return
	}

	cursor = key[DataKeyHeaderLength:]
	return
}

func SetDataValue(wb *bitskv.WriteBatch, key, member []byte, isCompress bool) (err error) {
	if isCompress {
		err = wb.PutMultiValue(key, []byte{DataValueKindFieldCompress}, member[KeyFieldCompressPrefix:])
	} else {
		err = wb.Put(key, []byte{DataValueKindDefault})
	}
	return err
}
