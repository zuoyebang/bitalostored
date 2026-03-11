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

package engine

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb"
	"github.com/zuoyebang/bitalostored/stored/engine/btools"
	"github.com/zuoyebang/bitalostored/stored/engine/dbconfig"
	"github.com/zuoyebang/bitalostored/stored/internal/config"

	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/stretchr/testify/require"
)

type BitalosDBMinCore struct {
	cfg        *dbconfig.Config
	db         *Bitalos
	removeFile bool
	dbPath     string
}

const testDBPath = "./test_cores"
const testCacheDBPath = "./test_cache_cores"
const testLogPath = "./test_log"

func testGetDefaultConfig() *dbconfig.Config {
	cfg := dbconfig.NewConfigDefault()
	cfg.VectorTableCount = 1
	cfg.VectorTableHashSize = 1
	return cfg
}

func testCacheDefaultConfig() *dbconfig.Config {
	cfg := dbconfig.NewConfigDefault()
	cfg.CacheSize = 200 << 20
	cfg.CacheHashSize = 10000
	return cfg
}

func testTwoBitsCores() []*BitalosDBMinCore {
	dbs := make([]*BitalosDBMinCore, 2)
	dbs[0] = testNewNoCacheBitsDB()
	dbs[1] = testNewCachedDB()
	return dbs
}

func (c *BitalosDBMinCore) Close() {
	c.db.Close()
	os.RemoveAll(c.dbPath)
	os.RemoveAll(testLogPath)
	config.GlobalConfig.Plugin.OpenRaft = true
}

func closeCores(cores []*BitalosDBMinCore) {
	for _, c := range cores {
		c.Close()
	}
	os.RemoveAll(testLogPath)
}

func testNewNoCacheBitsDB() *BitalosDBMinCore {
	dbPath := testDBPath
	os.RemoveAll(dbPath)
	cfg := testGetDefaultConfig()

	core := &BitalosDBMinCore{}
	core.dbPath = dbPath
	core.cfg = cfg
	core.db = testOpenBitsDb(true, dbPath, cfg)
	return core
}

func testNewCachedDB() *BitalosDBMinCore {
	dbPath := testCacheDBPath
	os.RemoveAll(dbPath)
	cfg := testCacheDefaultConfig()

	core := &BitalosDBMinCore{}
	core.dbPath = dbPath
	core.cfg = cfg
	core.db = testOpenBitsDb(true, dbPath, cfg)
	return core
}

func testOpenBitsDb(del bool, dbPath string, cfg *dbconfig.Config) *Bitalos {
	if del {
		os.RemoveAll(dbPath)
	}

	config.GlobalConfig.Plugin.OpenRaft = false
	cfg.DBPath = dbPath
	os.MkdirAll(dbPath, 0755)
	meta, err := NewMeta(dbPath)
	if err != nil {
		panic(err)
	}
	cfg.GetNextKeyId = meta.GetNextKeyUniqId
	cfg.GetCurrentKeyId = meta.GetCurrentKeyUniqId
	cfg.VectorTableCount = 1
	bdb, err1 := bitsdb.NewBitsDB(cfg)
	if err1 != nil {
		panic(err1)
	}
	b := &Bitalos{
		bitsdb: bdb,
		meta:   meta,
	}
	return b
}

func closeDb(b *Bitalos) {
	b.Close()
	os.RemoveAll(testDBPath)
	config.GlobalConfig.Plugin.OpenRaft = true
}

func testRandBytes(len int) []byte {
	val := make([]byte, len, len)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		val[i] = byte(b)
	}
	return val
}

func testMakeIntKey(n int) []byte {
	return []byte("key_prefix_" + strconv.Itoa(n))
}

type testItem struct {
	key     []byte
	keyHash uint32
	kvs     [][]byte
}

func testMakeItemList(kn, kkn int, dt uint8, ksize, vsize int) []*testItem {
	keys := make([]*testItem, kn)
	for i := 0; i < kn; i++ {
		keys[i] = &testItem{
			key: []byte(fmt.Sprintf("test_zset_key_%d", i)),
		}
		keys[i].keyHash = hash.Fnv32(keys[i].key)

		switch dt {
		case btools.DataTypeHash:
			skeys := make([][]byte, 0, kkn)
			for j := 0; j < kkn; j++ {
				skeys = append(skeys, testRandBytes(ksize))
			}
			sort.Slice(skeys, func(i, j int) bool {
				return bytes.Compare(skeys[i], skeys[j]) < 0
			})

			keys[i].kvs = make([][]byte, 0, kkn*2)
			for j := 0; j < kkn; j++ {
				keys[i].kvs = append(keys[i].kvs, skeys[j], testRandBytes(vsize))
			}
		case btools.DataTypeZset:
			skeys := make([][]byte, 0, kkn)
			for j := 0; j < kkn; j++ {
				skeys = append(skeys, testRandBytes(ksize))
			}
			sort.Slice(skeys, func(i, j int) bool {
				return bytes.Compare(skeys[i], skeys[j]) < 0
			})

			keys[i].kvs = make([][]byte, 0, kkn*2)
			for j := 0; j < kkn; j++ {
				keys[i].kvs = append(keys[i].kvs, getFloatByte(float64(j+1)), skeys[j])
			}
		default:
			skeys := make([][]byte, 0, kkn)
			for j := 0; j < kkn; j++ {
				skeys = append(skeys, testRandBytes(ksize))
			}
			sort.Slice(skeys, func(i, j int) bool {
				return bytes.Compare(skeys[i], skeys[j]) < 0
			})
			keys[i].kvs = skeys
		}
	}

	return keys
}

func TestDBRemoveSlot(t *testing.T) {
	db := testNewNoCacheBitsDB()
	defer db.Close()

	bdb := db.db

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("test_key_%d", i))
		require.NoError(t, bdb.Set(key, uint32(i), key))
	}

	bdb.bitsdb.DB.Flush(false)

	for i := 0; i < 10; i++ {
		require.NoError(t, bdb.bitsdb.DB.RemoveSlot(uint16(i)))
	}

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("test_key_%d", i))
		v, closer, err := bdb.Get(key, uint32(i))
		if closer != nil {
			closer()
		}
		fmt.Println("get", string(key), v, err)
	}
}
