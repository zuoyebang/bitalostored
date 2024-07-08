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

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/dbconfig"
)

func TestCache_New(t *testing.T) {
	dbPath := testCacheDBPath
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)
	cfg := dbconfig.NewConfigDefault()
	cfg.CacheSize = 10 << 20
	cfg.CacheHashSize = 10000
	db := testOpenBitsDb(true, dbPath, cfg)
	require.Equal(t, 1<<30, int(db.baseDb.MetaCache.MaxMem()))
	require.Equal(t, 1024, db.baseDb.MetaCache.Shards())
	db.Close()

	cfg.CacheSize = 200 << 20
	cfg.CacheEliminateDuration = 10
	cfg.CacheShardNum = 3
	db = testOpenBitsDb(true, dbPath, cfg)
	require.Equal(t, 1<<30, int(db.baseDb.MetaCache.MaxMem()))
	require.Equal(t, 1024, db.baseDb.MetaCache.Shards())
	db.Close()

	cfg.CacheSize = 1<<30 + 1<<20
	cfg.CacheShardNum = 1100
	db = testOpenBitsDb(true, dbPath, cfg)
	require.Equal(t, 1<<30+1<<20, int(db.baseDb.MetaCache.MaxMem()))
	require.Equal(t, 2048, db.baseDb.MetaCache.Shards())
	db.Close()
}
