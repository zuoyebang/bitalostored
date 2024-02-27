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

package kv

import "github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"

const (
	DB_TYPE_DATA int = 1 + iota
	DB_TYPE_META
	DB_TYPE_INDEX
	DB_TYPE_EXPIRE
)

const (
	DB_ID_NONE int = iota
	DB_ID_META
	DB_ID_EXPIRE
	DB_ID_HASH_DATA
	DB_ID_SET_DATA
	DB_ID_LIST_DATA
	DB_ID_ZSET_DATA
	DB_ID_ZSET_INDEX
)

const (
	DB_TYPE_DIR_META   string = "meta"
	DB_TYPE_DIR_EXPIRE string = "expire"
	DB_TYPE_DIR_DATA   string = "data"
	DB_TYPE_DIR_INDEX  string = "index"
)

func GetDbTypeDir(dbType int) string {
	switch dbType {
	case DB_TYPE_DATA:
		return DB_TYPE_DIR_DATA
	case DB_TYPE_META:
		return DB_TYPE_DIR_META
	case DB_TYPE_INDEX:
		return DB_TYPE_DIR_INDEX
	case DB_TYPE_EXPIRE:
		return DB_TYPE_DIR_EXPIRE
	default:
		return ""
	}
}

func GetDbId(kind btools.DataType, dbType int) int {
	switch dbType {
	case DB_TYPE_META:
		return DB_ID_META
	case DB_TYPE_EXPIRE:
		return DB_ID_EXPIRE
	case DB_TYPE_INDEX:
		switch kind {
		case btools.ZSET:
			return DB_ID_ZSET_INDEX
		default:
			return DB_ID_NONE
		}
	case DB_TYPE_DATA:
		switch kind {
		case btools.HASH:
			return DB_ID_HASH_DATA
		case btools.LIST:
			return DB_ID_LIST_DATA
		case btools.SET:
			return DB_ID_SET_DATA
		case btools.ZSET:
			return DB_ID_ZSET_DATA
		default:
			return DB_ID_NONE
		}
	default:
		return DB_ID_NONE
	}
}

func GetDbName(id int) string {
	switch id {
	case DB_ID_META:
		return "db/meta"
	case DB_ID_EXPIRE:
		return "db/expire"
	case DB_ID_HASH_DATA:
		return "db/hash"
	case DB_ID_LIST_DATA:
		return "db/list"
	case DB_ID_SET_DATA:
		return "db/set"
	case DB_ID_ZSET_DATA:
		return "db/zset"
	case DB_ID_ZSET_INDEX:
		return "db/zsetindex"
	default:
		return "none"
	}
}
