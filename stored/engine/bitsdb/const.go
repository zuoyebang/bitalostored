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

package bitsdb

import "github.com/zuoyebang/bitalosdb/v2"

const (
	DataDbDirname string = "bitvector"

	defaultCacheShardNum           int = 1024
	defaultCacheEliminateThreadNum int = 1
	defaultCacheEliminateDuration  int = 1080

	ErrnoKeyNotFoundOrExpire = -2
	ErrnoKeyPersist          = -1
)

var MissCacheValue = []byte{0xff, 0xff, 0xff, 0xff, 0xf7, 0x37, 0x51, 0xda}

var ErrWrongType = bitalosdb.ErrWrongType

var NilDataVal = []byte{0}
