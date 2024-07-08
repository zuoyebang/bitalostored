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

import "bytes"

type DataType uint8

const (
	NoneType DataType = iota
	STRING
	HASH
	LIST
	ZSETOLD
	SET
	ZSET
)

const (
	StringName  = "string"
	HashName    = "hash"
	ListName    = "list"
	ZSetName    = "zset"
	ZSetOldName = "zsetold"
	SetName     = "set"
)

var DataTypeList = []DataType{STRING, HASH, LIST, SET, ZSET}
var DataTypeNameList = []string{StringName, HashName, ListName, SetName, ZSetName}

func (d DataType) String() string {
	switch d {
	case STRING:
		return StringName
	case HASH:
		return HashName
	case LIST:
		return ListName
	case SET:
		return SetName
	case ZSET:
		return ZSetName
	case ZSETOLD:
		return ZSetOldName
	default:
		return ""
	}
}

func StringToDataType(t string) DataType {
	switch t {
	case StringName:
		return STRING
	case HashName:
		return HASH
	case ListName:
		return LIST
	case ZSetName:
		return ZSET
	case SetName:
		return SET
	default:
		return NoneType
	}
}

func IsDataTypeFieldCompress(dt DataType) bool {
	return dt == SET || dt == ZSET || dt == ZSETOLD
}

type ScanPair struct {
	Key []byte
	Dt  DataType
}

type KVPair struct {
	Key   []byte
	Value []byte
}

type FVPair struct {
	Field []byte
	Value []byte
}

type ScorePair struct {
	Score  float64
	Member []byte
}

type FieldPair struct {
	Prefix, Suffix []byte
}

func (fp FieldPair) Merge() []byte {
	var field []byte
	if len(fp.Prefix) > 0 {
		field = append(field, fp.Prefix...)
	}
	if len(fp.Suffix) > 0 {
		field = append(field, fp.Suffix...)
	}
	return field
}

func (fp FieldPair) Clone() FieldPair {
	var newFp FieldPair
	if len(fp.Prefix) > 0 {
		newFp.Prefix = append([]byte{}, fp.Prefix...)
	}
	if len(fp.Suffix) > 0 {
		newFp.Suffix = append([]byte{}, fp.Suffix...)
	}
	return newFp
}

func (fp FieldPair) Equal(field []byte) bool {
	prefixLen := len(fp.Prefix)
	flen := len(field)
	if flen < prefixLen {
		return false
	} else if flen == prefixLen {
		return bytes.Equal(fp.Prefix, field)
	} else {
		if !bytes.Equal(fp.Prefix, field[:prefixLen]) {
			return false
		}
		return bytes.Equal(fp.Suffix, field[prefixLen:])
	}
}
