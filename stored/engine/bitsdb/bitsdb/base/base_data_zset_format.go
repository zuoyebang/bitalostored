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
	"crypto/md5"
	"encoding/binary"

	"github.com/zuoyebang/bitalostored/butils/numeric"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/bytepools"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

func EncodeZsetDataKey(buf []byte, version uint64, khash uint32, member []byte, isOld bool) int {
	if !isOld {
		PutDataKeyHeader(buf, version, khash)
		memberMd5 := md5.Sum(member)
		copy(buf[DataKeyHeaderLength:DataKeyZsetLength], memberMd5[0:FieldMd5Length])
		return DataKeyZsetLength
	} else {
		var verBytes [8]byte
		binary.LittleEndian.PutUint16(buf, utils.GetSlotId(khash))
		binary.LittleEndian.PutUint64(verBytes[:], version)
		verMember := append(member, verBytes[:]...)
		verMemberMd5 := md5.Sum(verMember)
		copy(buf[keySlotIdLength:DataKeyZsetOldLength], verMemberMd5[0:FieldMd5Length])
		return DataKeyZsetOldLength
	}
}

func EncodeZsetIndexKeyScore(buf []byte, version uint64, khash uint32, score float64) {
	PutDataKeyHeader(buf, version, khash)
	numeric.Float64ToByteSort(score, buf[DataKeyHeaderLength:IndexKeyScoreLength])
}

func EncodeZsetIndexKeyUpperBound(buf []byte, version uint64, khash uint32) {
	PutDataKeyHeader(buf, version, khash)
	copy(buf[DataKeyHeaderLength:DataKeyHeaderLength+ScoreLength], btools.MaxScoreByte)
}

func EncodeZsetIndexKeyScoreUpperBound(buf []byte, version uint64, khash uint32, score float64) {
	PutDataKeyHeader(buf, version, khash)
	numeric.Float64ToByteSort(score, buf[DataKeyHeaderLength:IndexKeyScoreLength])
	copy(buf[IndexKeyScoreLength:IndexKeyScoreUpperBoundLength], MaxUpperBound)
}

func EncodeZsetIndexKey(version uint64, kind uint8, khash uint32, score float64, member []byte) ([]byte, func(), bool) {
	memberSize := len(member)
	isCompress := false
	if kind == KeyKindFieldCompress && memberSize > KeyFieldCompressSize {
		isCompress = true
		memberSize = KeyFieldCompressPrefix + FieldMd5Length
	}

	size := IndexKeyScoreLength + memberSize
	buf, closer := bytepools.BytePools.GetBytePool(size)

	PutDataKeyHeader(buf, version, khash)
	numeric.Float64ToByteSort(score, buf[DataKeyHeaderLength:IndexKeyScoreLength])

	if memberSize > 0 {
		pos := IndexKeyScoreLength
		if isCompress {
			copy(buf[pos:pos+KeyFieldCompressPrefix], member[0:KeyFieldCompressPrefix])
			memberMd5 := md5.Sum(member)
			copy(buf[pos+KeyFieldCompressPrefix:], memberMd5[:])
		} else {
			copy(buf[pos:], member)
		}
	}

	return buf[:size], closer, isCompress
}

func DecodeZsetIndexKey(keyKind uint8, key, value []byte) (version uint64, score float64, member btools.FieldPair) {
	if len(key) <= DataKeyHeaderLength {
		return
	}

	pos := keySlotIdLength
	version = binary.LittleEndian.Uint64(key[pos : pos+keyVersionLength])
	pos += keyVersionLength
	score = numeric.ByteSortToFloat64(key[pos : pos+ScoreLength])
	pos += ScoreLength
	if len(value) > 0 {
		isCompress := IsDataValueFieldCompress(keyKind, value)
		if !isCompress {
			member.Prefix = key[pos:]
			member.Suffix = nil
		} else {
			member.Prefix = key[pos : pos+KeyFieldCompressPrefix]
			member.Suffix = value[DataValueFieldOffset:]
		}
	}

	return
}

func EncodeZsetIndexKeyByCursor(version uint64, khash uint32, cursor []byte) ([]byte, func()) {
	return EncodeDataKeyCursor(version, khash, cursor)
}

func DecodeZsetIndexKeyByCursor(
	keyKind uint8, key, value []byte,
) (version uint64, score float64, member btools.FieldPair, cursor []byte) {
	version, score, member = DecodeZsetIndexKey(keyKind, key, value)
	if version == 0 {
		return
	}

	cursor = key[DataKeyHeaderLength:]
	return
}
