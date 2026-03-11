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

package btools

import (
	"bytes"

	"github.com/zuoyebang/bitalosdb/v2"
	"github.com/zuoyebang/bitalostored/stored/internal/bytepools"
)

const (
	DataTypeNone   = 0
	DataTypeString = bitalosdb.DataTypeString
	DataTypeBitmap = bitalosdb.DataTypeBitmap
	DataTypeHash   = bitalosdb.DataTypeHash
	DataTypeList   = bitalosdb.DataTypeList
	DataTypeSet    = bitalosdb.DataTypeSet
	DataTypeZset   = bitalosdb.DataTypeZset
	DataTypeDKHash = bitalosdb.DataTypeDKHash
	DataTypeDKSet  = bitalosdb.DataTypeDKSet
)

const (
	DataTypeStringName = "string"
	DataTypeHashName   = "hash"
	DataTypeListName   = "list"
	DataTypeSetName    = "set"
	DataTypeZsetName   = "zset"
)

var DataTypeNameMap = map[uint8]string{
	DataTypeString: DataTypeStringName,
	DataTypeBitmap: DataTypeStringName,
	DataTypeHash:   DataTypeHashName,
	DataTypeList:   DataTypeListName,
	DataTypeSet:    DataTypeSetName,
	DataTypeZset:   DataTypeZsetName,
}

var DataTypeNameList = []string{
	DataTypeStringName,
	DataTypeHashName,
	DataTypeListName,
	DataTypeSetName,
	DataTypeZsetName,
}

func StringToDataType(t string) uint8 {
	switch t {
	case DataTypeStringName:
		return DataTypeString
	case DataTypeHashName:
		return DataTypeHash
	case DataTypeListName:
		return DataTypeList
	case DataTypeSetName:
		return DataTypeSet
	case DataTypeZsetName:
		return DataTypeZset
	default:
		return DataTypeNone
	}
}

type ScanPair struct {
	Key []byte
	Dt  uint8
}

type KVPair struct {
	Key   []byte
	Value []byte
}

type FVPair struct {
	Field []byte
	Value []byte
}

type ScorePair = bitalosdb.ScorePair

type ZsetPair struct {
	FieldPair
	Score float64
}

type FieldPair struct {
	Prefix, Suffix []byte
}

func (fp FieldPair) MergeByPool() ([]byte, func()) {
	if len(fp.Prefix) == 0 {
		return nil, nil
	}

	pLen := len(fp.Prefix)
	sLen := len(fp.Suffix)
	size := pLen + sLen
	buf, closer := bytepools.GlobalBytePools.GetBytePool(size)
	copy(buf[:pLen], fp.Prefix)
	if sLen > 0 {
		copy(buf[pLen:size], fp.Suffix)
	}

	return buf[:size], closer
}

func (fp FieldPair) Merge() []byte {
	var key []byte
	if len(fp.Prefix) > 0 {
		key = append(key, fp.Prefix...)
	}
	if len(fp.Suffix) > 0 {
		key = append(key, fp.Suffix...)
	}
	return key
}

func (fp FieldPair) Equal(field []byte) bool {
	prefixLen := len(fp.Prefix)
	fieldLen := len(field)
	if fieldLen < prefixLen {
		return false
	} else if fieldLen == prefixLen {
		return bytes.Equal(fp.Prefix, field)
	} else {
		if !bytes.Equal(fp.Prefix, field[:prefixLen]) {
			return false
		}
		return bytes.Equal(fp.Suffix, field[prefixLen:])
	}
}

func (fp FieldPair) Compare(field []byte) int {
	if fp.Prefix != nil && fp.Suffix == nil {
		return bytes.Compare(field, fp.Prefix)
	}
	prefixLen := len(fp.Prefix)
	if prefixLen <= len(field) {
		if cmp := bytes.Compare(field[:prefixLen], fp.Prefix); cmp != 0 {
			return cmp
		} else {
			return bytes.Compare(field[prefixLen:], fp.Suffix)
		}
	} else {
		return bytes.Compare(field, fp.Prefix)
	}
}
