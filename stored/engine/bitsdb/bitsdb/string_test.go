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
	"bytes"
	"crypto/md5"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/dbconfig"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/dbmeta"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"
)

type BitalosDBMinCore struct {
	cfg        *dbconfig.Config
	db         *BitsDB
	removeFile bool
	dbPath     string
}

const testDBPath = "./test_cores"
const testCacheDBPath = "./test_cache_cores"

func testGetDefaultConfig() *dbconfig.Config {
	cfg := dbconfig.NewConfigDefault()
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
	config.GlobalConfig.Plugin.OpenRaft = true
}

func closeCores(cores []*BitalosDBMinCore) {
	for _, c := range cores {
		c.Close()
	}
}

func testNewBitsDB() *BitsDB {
	os.RemoveAll(testDBPath)
	cfg := testGetDefaultConfig()
	return testOpenBitsDb(true, testDBPath, cfg)
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

func testNewBitsDBNoDel() *BitsDB {
	cfg := testGetDefaultConfig()
	return testOpenBitsDb(false, testDBPath, cfg)
}

func testOpenBitsDb(del bool, dbPath string, cfg *dbconfig.Config) *BitsDB {
	config.GlobalConfig.Plugin.OpenRaft = false
	cfg.DBPath = dbPath
	if del {
		os.RemoveAll(dbPath)
	}

	if err := os.MkdirAll(dbPath, 0755); err != nil {
		panic(err)
	}

	meta, err := dbmeta.OpenMeta(dbPath)
	if err != nil {
		panic(err)
	}

	bdb, err := NewBitsDB(cfg, meta)
	if err != nil {
		panic(err)
	}

	return bdb
}

func closeDb(b *BitsDB) {
	b.Close()
	os.RemoveAll(testDBPath)
	config.GlobalConfig.Plugin.OpenRaft = true
}

func testCheckKeyValue(t *testing.T, b *BitsDB, key []byte, khash uint32, value []byte) {
	v, closer, err := b.StringObj.Get(key, khash)
	if err != nil {
		t.Fatal(err)
	}
	if value == nil {
		if v != nil {
			t.Fatal("find not exist key value is not nil", string(key))
		}
	} else {
		if !bytes.Equal(v, value) {
			t.Fatal("v not eq", string(key), v, value)
			// } else if closer == nil {
			// 	t.Fatal("closer return nil", string(key))
		}
	}
	if closer != nil {
		closer()
	}
}

func TestKVEncode(t *testing.T) {
	key := []byte("testdb_key")
	ek, ekCloser := base.EncodeMetaKey(key, hash.Fnv32(key))
	defer ekCloser()
	pos, err := base.CheckMetaKey(ek)
	if err != nil {
		t.Fatal(err)
	}
	k := ek[pos:]
	if string(ek[pos:]) != "testdb_key" {
		t.Fatal(string(k))
	}
}

func TestKVReopenDb(t *testing.T) {
	bdb := testNewBitsDBNoDel()

	key := []byte("a")
	khash := hash.Fnv32(key)
	value := []byte("1")
	if err := bdb.StringObj.Set(key, khash, value); err != nil {
		t.Fatal(err)
	}
	testCheckKeyValue(t, bdb, key, khash, value)
	bdb.Close()

	bdb = testNewBitsDBNoDel()
	testCheckKeyValue(t, bdb, key, khash, value)
	closeDb(bdb)
}

func TestKVCmd(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("b")
		khash := hash.Fnv32(key)
		if err := bdb.StringObj.Set(key, khash, []byte("")); err != nil {
			t.Fatal(err)
		}
		testCheckKeyValue(t, bdb, key, khash, []byte(""))
		if err := bdb.StringObj.Set(key, khash, base.NilDataVal); err != nil {
			t.Fatal(err)
		}
		testCheckKeyValue(t, bdb, key, khash, base.NilDataVal)

		if n, err := bdb.StringObj.Del(khash, key); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal("del return not 1", string(key))
		}
		if n, err := bdb.StringObj.Del(khash, key); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal("del return not 0", string(key))
		}
		testCheckKeyValue(t, bdb, key, khash, nil)

		key1 := []byte("testdb_kv_a")
		k1hash := hash.Fnv32(key1)
		if err := bdb.StringObj.Set(key1, k1hash, []byte("hello world 1")); err != nil {
			t.Fatal(err)
		}

		key2 := []byte("testdb_kv_b")
		k2hash := hash.Fnv32(key2)
		if err := bdb.StringObj.Set(key2, k2hash, []byte("hello world 2")); err != nil {
			t.Fatal(err)
		}

		key3 := []byte("testdb_kv_c")
		k3hash := hash.Fnv32(key3)
		ay, mgetClosers, _ := bdb.StringObj.MGet(k1hash, key1, key2, key3)
		if v1 := ay[0]; string(v1) != "hello world 1" {
			t.Fatal("mget key1 val err")
		}
		if v2 := ay[1]; string(v2) != "hello world 2" {
			t.Fatal("mget key2 val err")
		}
		if v3 := ay[2]; v3 != nil {
			t.Fatal("mget key3 need nil")
		}

		for _, f := range mgetClosers {
			if f != nil {
				f()
			}
		}

		if n, err := bdb.StringObj.Append(key3, k3hash, []byte("Hello")); err != nil {
			t.Fatal(err)
		} else if n != 5 {
			t.Fatal(n)
		}

		if n, err := bdb.StringObj.Append(key3, k3hash, []byte(" World")); err != nil {
			t.Fatal(err)
		} else if n != 11 {
			t.Fatal(n)
		}

		if n, err := bdb.StringObj.StrLen(key3, k3hash); err != nil {
			t.Fatal(err)
		} else if n != 11 {
			t.Fatal(n)
		}

		v, closer, err := bdb.StringObj.GetRange(key3, k3hash, 0, 4)
		if err != nil {
			t.Fatal(err)
		} else if string(v) != "Hello" {
			t.Fatal(string(v))
		}
		if closer != nil {
			closer()
		}

		v, closer, err = bdb.StringObj.GetRange(key3, k3hash, 0, -1)
		if err != nil {
			t.Fatal(err)
		} else if string(v) != "Hello World" {
			t.Fatal(string(v))
		}
		if closer != nil {
			closer()
		}

		v, closer, err = bdb.StringObj.GetRange(key3, k3hash, -5, -1)
		if err != nil {
			t.Fatal(err)
		} else if string(v) != "World" {
			t.Fatal(string(v))
		}
		if closer != nil {
			closer()
		}

		if n, err := bdb.StringObj.SetRange(key3, k3hash, 6, []byte("Redis")); err != nil {
			t.Fatal(err)
		} else if n != 11 {
			t.Fatal(n)
		}
		testCheckKeyValue(t, bdb, key3, k3hash, []byte("Hello Redis"))

		key4 := []byte("testdb_kv_range_none")
		k4hash := hash.Fnv32(key4)
		if n, err := bdb.StringObj.SetRange(key4, k4hash, 6, []byte("Redis")); err != nil {
			t.Fatal(err)
		} else if n != 11 {
			t.Fatal(n)
		}
		testCheckKeyValue(t, bdb, key4, k4hash, []byte{0, 0, 0, 0, 0, 0, 82, 101, 100, 105, 115})

		key5 := []byte("testdb_kv_bit")
		k5hash := hash.Fnv32(key5)
		if n, err := bdb.StringObj.SetBit(key5, k5hash, 7, 1); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if n, err := bdb.StringObj.GetBit(key5, k5hash, 0); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
		if n, err := bdb.StringObj.GetBit(key5, k5hash, 7); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.StringObj.GetBit(key5, k5hash, 100); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if n, err := bdb.StringObj.BitCount(key5, k5hash, 0, -1); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := bdb.StringObj.BitPos(key5, k5hash, 1, 0, -1); err != nil {
			t.Fatal(err)
		} else if n != 7 {
			t.Fatal(n)
		}

		if n, err := bdb.StringObj.SetBit(key5, k5hash, 7, 0); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.StringObj.GetBit(key5, k5hash, 7); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		key6 := []byte("testdb_kv_bitop_desc")
		k6hash := hash.Fnv32(key6)
		k6value := []byte("fooba")
		testCheckKeyValue(t, bdb, key6, k6hash, nil)
		if err = bdb.StringObj.Set(key6, k6hash, k6value); err != nil {
			t.Fatal(err)
		}
		testCheckKeyValue(t, bdb, key6, k6hash, k6value)

		key7 := []byte("a")
		k7hash := hash.Fnv32(key7)
		k7value := []byte("a")
		if err = bdb.StringObj.Set(key7, k7hash, k7value); err != nil {
			t.Fatal(err)
		}
		testCheckKeyValue(t, bdb, key7, k7hash, k7value)

		key8 := []byte("incrkey")
		k8hash := hash.Fnv32(key8)
		if res, err := bdb.StringObj.Incr(key8, k8hash); err != nil {
			t.Fatal(err)
		} else if res != 1 {
			t.Fatal(res)
		}
		if res, err := bdb.StringObj.IncrBy(key8, k8hash, 10); err != nil {
			t.Fatal(err)
		} else if res != 11 {
			t.Fatal(res)
		}
		if res, err := bdb.StringObj.Decr(key8, k8hash); err != nil {
			t.Fatal(err)
		} else if res != 10 {
			t.Fatal(res)
		}
		if res, err := bdb.StringObj.DecrBy(key8, k8hash, 2); err != nil {
			t.Fatal(err)
		} else if res != 8 {
			t.Fatal(res)
		}
		if res, err := bdb.StringObj.IncrByFloat(key8, k8hash, 10.2); err != nil {
			t.Fatal(err)
		} else if res != 18.2 {
			t.Fatal(res)
		}
		v, closer, err = bdb.StringObj.Get(key8, k8hash)
		if err != nil {
			t.Fatal(err)
		} else if string(v) != "18.2" {
			t.Fatal(string(v))
			// } else if closer == nil {
			// 	t.Fatal("key8 return closer is nil")
		} else {
			if closer != nil {
				closer()
			}
		}
	}
}

func TestKVSetEX(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		checkEx := func(key []byte) {
			value1 := []byte("hello world1")
			value2 := []byte("hello world2")
			khash := hash.Fnv32(key)
			if err := bdb.StringObj.Set(key, khash, value1); err != nil {
				t.Fatal(err)
			}
			if err := bdb.StringObj.SetEX(key, khash, 10, value1, false); err != nil {
				t.Fatal(err)
			}
			if n, err := bdb.StringObj.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n < 9 {
				t.Fatal(n)
			}
			testCheckKeyValue(t, bdb, key, khash, value1)

			if err := bdb.StringObj.SetEX(key, khash, 9200, value2, true); err != nil {
				t.Fatal(err)
			}
			if n, err := bdb.StringObj.PTTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 9200 {
				t.Fatal(n)
			}
			if n, err := bdb.StringObj.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n < 9 {
				t.Fatal(n)
			}
			testCheckKeyValue(t, bdb, key, khash, value2)

			if err := bdb.StringObj.SetEX(key, khash, 900, value2, true); err != nil {
				t.Fatal(err)
			}
			time.Sleep(1000 * time.Millisecond)
			testCheckKeyValue(t, bdb, key, khash, nil)
			if n, err := bdb.StringObj.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != base.ErrnoKeyNotFoundOrExpire {
				t.Fatal(n)
			}
		}

		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("testdb_kv_setex_%d", i))
			checkEx(key)
		}
	}
}

func TestMSetAndDel(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key1 := []byte("mset_key1")
		key2 := []byte("mset_key2")
		key3 := []byte("mset_key3")

		keys := make([]btools.KVPair, 0, 3)
		keys = append(keys, btools.KVPair{Key: key1, Value: []byte("a")})
		keys = append(keys, btools.KVPair{Key: key2, Value: []byte("b")})
		keys = append(keys, btools.KVPair{Key: key3, Value: []byte("c")})

		k1hash := hash.Fnv32(key1)
		if err := bdb.StringObj.MSet(k1hash, keys...); err != nil {
			t.Fatal(err)
		}

		v, mgetClosers, err := bdb.StringObj.MGet(k1hash, key1, key2, key3)
		if err != nil {
			t.Fatal(err)
		} else if string(v[0]) != "a" {
			t.Fatal("v is nil")
		} else if string(v[1]) != "b" {
			t.Fatal("v is nil")
		} else if string(v[2]) != "c" {
			t.Fatal("v is nil")
		}

		for _, closer := range mgetClosers {
			if closer != nil {
				closer()
			}
		}

		n, err := bdb.StringObj.Del(k1hash, key1)
		if err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		k2hash := hash.Fnv32(key2)
		err = bdb.StringObj.SetEX(key2, k2hash, 1, []byte("b1"), false)
		if err != nil {
			t.Fatal(err)
		}

		v, mgetClosers, err = bdb.StringObj.MGet(k1hash, key1, key2, key3)
		if err != nil {
			t.Fatal(err)
		} else if v[0] != nil {
			t.Fatal(len(v))
		} else if !bytes.Equal([]byte("b1"), v[1]) {
			t.Fatal(v[1])
		} else if string(v[2]) != "c" {
			t.Fatal("v is nil")
		}
		for _, closer := range mgetClosers {
			if closer != nil {
				closer()
			}
		}

		time.Sleep(2 * time.Second)
		v, mgetClosers, err = bdb.StringObj.MGet(k1hash, key1, key2, key3)
		if err != nil {
			t.Fatal(err)
		} else if v[0] != nil {
			t.Fatal(len(v))
		} else if v[1] != nil {
			t.Fatal(v[1])
		} else if string(v[2]) != "c" {
			t.Fatal("v is nil")
		}
		for _, closer := range mgetClosers {
			if closer != nil {
				closer()
			}
		}
	}
}

func TestKVSetBitLen(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("TestKVSetBitLen")
		khash := hash.Fnv32(key)
		n, err := bdb.StringObj.SetBit(key, khash, 123456, 1)
		require.NoError(t, err)
		require.Equal(t, int64(0), n)
		n, err = bdb.StringObj.GetBit(key, khash, 123456)
		require.NoError(t, err)
		require.Equal(t, int64(1), n)

		n, err = bdb.StringObj.SetBit(key, khash, 123457, 1)
		require.NoError(t, err)
		require.Equal(t, int64(0), n)

		l, err := bdb.StringObj.StrLen(key, khash)
		require.NoError(t, err)
		require.Equal(t, int64(32), l)

		n, err = bdb.StringObj.GetBit(key, khash, 123456)
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
	}
}

func TestKVSetBitGetBit(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("TestKVSetBitGetBit")
		khash := hash.Fnv32(key)

		cases := []struct {
			offset, on     int
			setexp, getexp int64
		}{
			{0, 0, 0, 0},
			{0, 1, 0, 1},
			{0, 1, 1, 1},
			{123, 0, 0, 0},
			{123, 1, 0, 1},
			{123, 1, 1, 1},
			{1234, 1, 0, 1},
			{1234, 0, 1, 0},
			{1234, 0, 0, 0},
			{math.MaxInt64, 1, 0, 1},
			{math.MaxInt64, 0, 1, 0},
			{math.MaxInt64, 0, 0, 0},
		}

		t.Run("test setbit and getbit", func(t *testing.T) {
			for _, c := range cases {
				n, err := bdb.StringObj.SetBit(key, khash, c.offset, c.on)
				require.NoError(t, err)
				require.Equal(t, c.setexp, n)

				n, err = bdb.StringObj.GetBit(key, khash, c.offset)
				require.NoError(t, err)
				require.Equal(t, c.getexp, n)
			}
		})
	}
}

func TestKVSetBit(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := "test_bitmap"
		khash := hash.Fnv32([]byte(key))
		var kvSetBitBase int64 = 0
		for i := 0; i < 100; i++ {
			n := atomic.AddInt64(&kvSetBitBase, 1)
			num := n / 10
			key += strconv.FormatInt(num, 10)
			wkey := md5.Sum([]byte(key))
			_, err := bdb.StringObj.SetBit(wkey[0:16], khash, int(n), int(n%2))
			require.NoError(t, err)
		}
	}
}

func TestKVSetBitDelete(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("TestKVSetBitDelete")
		khash := hash.Fnv32(key)
		n, err := bdb.StringObj.SetBit(key, khash, 1, 1)
		require.NoError(t, err)
		require.Equal(t, int64(0), n)
		ex, err := bdb.StringObj.Exists(key, khash)
		require.NoError(t, err)
		require.Equal(t, int64(1), ex)

		n, err = bdb.StringObj.SetBit(key, khash, 1, 0)
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
		ex, err = bdb.StringObj.Exists(key, khash)
		require.NoError(t, err)
		require.Equal(t, int64(0), ex)
	}
}

func TestKVBitCount(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("TestKVBitCount")
		khash := hash.Fnv32(key)

		n, err := bdb.StringObj.BitCount(key, khash, 0, -1)
		require.NoError(t, err)
		require.Equal(t, int64(0), n)

		for i := 110; i <= 120; i++ {
			n, err = bdb.StringObj.SetBit(key, khash, i, 1)
			require.NoError(t, err)
			require.Equal(t, int64(0), n)
		}

		cases := []struct {
			start, end int
			exp        int64
		}{
			{0, -1, 11},
			{109, 130, 11},
			{109, 113, 4},
			{111, 113, 3},
			{109, 130, 11},
			{119, 130, 2},
			{129, 140, 0},
			{119, -2, 2},
			{-1, -10, 0},
		}

		t.Run("test bitcount", func(t *testing.T) {
			for _, c := range cases {
				n, err = bdb.StringObj.BitCount(key, khash, c.start, c.end)
				require.NoError(t, err)
				require.Equal(t, c.exp, n)
			}
		})
	}
}

func TestKVBitPos(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("TestKVBitPos")
		khash := hash.Fnv32(key)
		n, err := bdb.StringObj.BitPos(key, khash, 1, 0, -1)
		require.NoError(t, err)
		require.Equal(t, int64(-1), n)
		n, err = bdb.StringObj.BitPos(key, khash, 0, 0, -1)
		require.NoError(t, err)
		require.Equal(t, int64(0), n)

		for i := 110; i <= 120; i++ {
			n, err = bdb.StringObj.SetBit(key, khash, i, 1)
			require.NoError(t, err)
			require.Equal(t, int64(0), n)
		}

		n, err = bdb.StringObj.SetBit(key, khash, 125, 1)
		require.NoError(t, err)
		require.Equal(t, int64(0), n)

		cases := []struct {
			start, end int
			exp1, exp0 int64
		}{
			{0, -1, 110, 0},
			{109, 130, 110, 109},
			{109, 113, 110, 109},
			{110, 113, 110, 114},
			{110, 130, 110, 121},
			{109, 130, 110, 109},
			{119, 130, 119, 121},
			{129, 140, -1, 129},
			{119, -2, 119, 121},
			{-10, -1, -1, 9223372036854775798},
			{-1, -10, -1, -1},
		}

		t.Run("test bitpos", func(t *testing.T) {
			for _, c := range cases {
				n, err = bdb.StringObj.BitPos(key, khash, 1, c.start, c.end)
				require.NoError(t, err)
				require.Equal(t, c.exp1, n)

				n, err = bdb.StringObj.BitPos(key, khash, 0, c.start, c.end)
				require.NoError(t, err)
				require.Equal(t, c.exp0, n)
			}
		})
	}
}

func TestKVExpire(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		var res int64
		var err error
		expExpire := true

		newTime := tclock.GetTimestampSecond() + 200

		checkKey := func(key, val []byte, exist bool, op string) {
			khash := hash.Fnv32(key)
			value, vcloser, err := bdb.StringObj.Get(key, khash)
			defer func() {
				if vcloser != nil {
					vcloser()
				}
			}()
			require.NoError(t, err)
			require.Equal(t, val, value)
		}

		key1 := []byte("key1")
		k1hash := hash.Fnv32(key1)
		val1 := []byte("val1")
		if err = bdb.StringObj.Set(key1, k1hash, val1); err != nil {
			t.Fatal(err)
		}
		checkKey(key1, val1, false, "Set key1")
		if res, err = bdb.StringObj.ExpireAt(key1, k1hash, newTime); err != nil {
			t.Fatal(err)
		} else if res != 1 {
			t.Fatal("ExpireAt key1 fail")
		}
		checkKey(key1, val1, expExpire, "ExpireAt key1")
		if res, err = bdb.StringObj.BasePersist(key1, k1hash); err != nil {
			t.Fatal(err)
		} else if res != 1 {
			t.Fatal("Persist fail")
		}
		checkKey(key1, val1, false, "Persist key1")

		key2 := []byte("key2")
		k2hash := hash.Fnv32(key2)
		val2 := []byte("key2")
		if err = bdb.StringObj.SetEX(key2, k2hash, 1000, val2, false); err != nil {
			t.Fatal(err)
		}
		checkKey(key2, val2, expExpire, "SetEX key2")
		if res, err = bdb.StringObj.Expire(key2, k2hash, 500); err != nil {
			t.Fatal(err)
		} else if res != 1 {
			t.Fatal("Expire key2 fail")
		}
		checkKey(key2, val2, expExpire, "Expire key2")
		if err = bdb.StringObj.SetEX(key2, k2hash, 1000, val2, false); err != nil {
			t.Fatal(err)
		}
		checkKey(key2, val2, expExpire, "SetEX key2 2")

		key3 := []byte("key3")
		k3hash := hash.Fnv32(key3)
		val3 := []byte("key3")
		if res, err = bdb.StringObj.SetNXEX(key3, k3hash, 2000, val3, false); err != nil {
			t.Fatal(err)
		} else if res != 1 {
			t.Fatal("key3 SetNXEX fail")
		}
		checkKey(key3, val3, expExpire, "SetNXEX key3")
		if res, err = bdb.StringObj.Del(k3hash, key3); err != nil {
			t.Fatal(err)
		}
		checkKey(key3, []byte(nil), false, "Del key3")
		if err = bdb.StringObj.Set(key3, k3hash, val3); err != nil {
			t.Fatal(err)
		}
		checkKey(key3, val3, false, "Set key3")
		if err = bdb.StringObj.Set(key3, k3hash, val3); err != nil {
			t.Fatal(err)
		}
		checkKey(key3, val3, false, "Set key3")
		if err = bdb.StringObj.SetEX(key3, k3hash, 1000, val3, false); err != nil {
			t.Fatal(err)
		}
		checkKey(key3, val3, expExpire, "SetEX key3")
		if res, err = bdb.StringObj.BasePersist(key3, k3hash); err != nil {
			t.Fatal(err)
		} else if res != 1 {
			t.Fatal("Persist fail")
		}
		checkKey(key3, val3, false, "Persist key3")
		if res, err = bdb.StringObj.Del(k3hash, key3); err != nil {
			t.Fatal(err)
		}
		checkKey(key3, []byte(nil), false, "Del key3 2")
	}
}
