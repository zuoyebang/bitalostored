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
	"crypto/md5"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb"
	"github.com/zuoyebang/bitalostored/stored/engine/btools"
	"github.com/zuoyebang/bitalostored/stored/engine/dbconfig"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/stretchr/testify/require"
)

func testNewBitsDB() *Bitalos {
	os.RemoveAll(testDBPath)
	cfg := testGetDefaultConfig()
	return testOpenBitsDb(true, testDBPath, cfg)
}

func testNewBitsDBNoDel() *Bitalos {
	cfg := testGetDefaultConfig()
	return testOpenBitsDb(false, testDBPath, cfg)
}

func testCheckKeyValue(t *testing.T, b *Bitalos, key []byte, khash uint32, value []byte) {
	v, closer, err := b.Get(key, khash)
	if err != nil {
		t.Fatal(err)
	}
	if value == nil {
		if v != nil {
			t.Fatal("find not exist key value is not nil", string(key), v)
		}
	} else if !bytes.Equal(v, value) {
		t.Fatal("v not eq", string(key), v, value)
	}
	if closer != nil {
		closer()
	}
}

func TestKVReopenDb(t *testing.T) {
	bdb := testNewBitsDBNoDel()

	key := []byte("a")
	khash := hash.Fnv32(key)
	value := []byte("1")
	if err := bdb.Set(key, khash, value); err != nil {
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
		if err := bdb.Set(key, khash, []byte("")); err != nil {
			t.Fatal(err)
		}
		testCheckKeyValue(t, bdb, key, khash, []byte(""))
		if err := bdb.Set(key, khash, bitsdb.NilDataVal); err != nil {
			t.Fatal(err)
		}
		testCheckKeyValue(t, bdb, key, khash, bitsdb.NilDataVal)

		if n, err := bdb.Del(khash, key); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal("del return not 1", string(key))
		}
		if n, err := bdb.Del(khash, key); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal("del return not 0", string(key))
		}
		testCheckKeyValue(t, bdb, key, khash, nil)

		key1 := []byte("testdb_kv_a")
		k1hash := hash.Fnv32(key1)
		require.NoError(t, bdb.Set(key1, k1hash, []byte("hello world 1")))

		key2 := []byte("testdb_kv_b")
		k2hash := hash.Fnv32(key2)
		require.NoError(t, bdb.Set(key2, k2hash, []byte("hello world 2")))

		key3 := []byte("testdb_kv_c")
		k3hash := hash.Fnv32(key3)
		ay, mgetClosers, _ := bdb.MGet(k1hash, key1, key2, key3)
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

		if n, err := bdb.Append(key3, k3hash, []byte("Hello")); err != nil {
			t.Fatal(err)
		} else if n != 5 {
			t.Fatal(n)
		}

		if n, err := bdb.Append(key3, k3hash, []byte(" World")); err != nil {
			t.Fatal(err)
		} else if n != 11 {
			t.Fatal(n)
		}

		if n, err := bdb.StrLen(key3, k3hash); err != nil {
			t.Fatal(err)
		} else if n != 11 {
			t.Fatal(n)
		}

		v, closer, err := bdb.GetRange(key3, k3hash, 0, 4)
		if err != nil {
			t.Fatal(err)
		} else if string(v) != "Hello" {
			t.Fatal(string(v))
		}
		if closer != nil {
			closer()
		}

		v, closer, err = bdb.GetRange(key3, k3hash, 0, -1)
		if err != nil {
			t.Fatal(err)
		} else if string(v) != "Hello World" {
			t.Fatal(string(v))
		}
		if closer != nil {
			closer()
		}

		v, closer, err = bdb.GetRange(key3, k3hash, -5, -1)
		if err != nil {
			t.Fatal(err)
		} else if string(v) != "World" {
			t.Fatal(string(v))
		}
		if closer != nil {
			closer()
		}

		if n, err := bdb.SetRange(key3, k3hash, 6, []byte("Redis")); err != nil {
			t.Fatal(err)
		} else if n != 11 {
			t.Fatal(n)
		}
		testCheckKeyValue(t, bdb, key3, k3hash, []byte("Hello Redis"))

		key4 := []byte("testdb_kv_range_none")
		k4hash := hash.Fnv32(key4)
		if n, err := bdb.SetRange(key4, k4hash, 6, []byte("Redis")); err != nil {
			t.Fatal(err)
		} else if n != 11 {
			t.Fatal(n)
		}
		testCheckKeyValue(t, bdb, key4, k4hash, []byte{0, 0, 0, 0, 0, 0, 82, 101, 100, 105, 115})

		key5 := []byte("testdb_kv_bit")
		k5hash := hash.Fnv32(key5)
		if n, err := bdb.SetBit(key5, k5hash, 7, 1); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if n, err := bdb.GetBit(key5, k5hash, 0); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
		if n, err := bdb.GetBit(key5, k5hash, 7); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.GetBit(key5, k5hash, 100); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if n, err := bdb.BitCount(key5, k5hash, 0, -1); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := bdb.BitPos(key5, k5hash, 1, 0, -1); err != nil {
			t.Fatal(err)
		} else if n != 7 {
			t.Fatal(n)
		}

		if n, err := bdb.SetBit(key5, k5hash, 7, 0); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.GetBit(key5, k5hash, 7); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		key6 := []byte("testdb_kv_bitop_desc")
		k6hash := hash.Fnv32(key6)
		k6value := []byte("fooba")
		testCheckKeyValue(t, bdb, key6, k6hash, nil)
		if err = bdb.Set(key6, k6hash, k6value); err != nil {
			t.Fatal(err)
		}
		testCheckKeyValue(t, bdb, key6, k6hash, k6value)

		key7 := []byte("a")
		k7hash := hash.Fnv32(key7)
		k7value := []byte("a")
		if err = bdb.Set(key7, k7hash, k7value); err != nil {
			t.Fatal(err)
		}
		testCheckKeyValue(t, bdb, key7, k7hash, k7value)

		key8 := []byte("incrkey")
		k8hash := hash.Fnv32(key8)
		if res, err := bdb.Incr(key8, k8hash); err != nil {
			t.Fatal(err)
		} else if res != 1 {
			t.Fatal(res)
		}
		if res, err := bdb.IncrBy(key8, k8hash, 10); err != nil {
			t.Fatal(err)
		} else if res != 11 {
			t.Fatal(res)
		}
		if res, err := bdb.Decr(key8, k8hash); err != nil {
			t.Fatal(err)
		} else if res != 10 {
			t.Fatal(res)
		}
		if res, err := bdb.DecrBy(key8, k8hash, 2); err != nil {
			t.Fatal(err)
		} else if res != 8 {
			t.Fatal(res)
		}
		if res, err := bdb.IncrByFloat(key8, k8hash, 10.2); err != nil {
			t.Fatal(err)
		} else if res != 18.2 {
			t.Fatal(res)
		}
		v, closer, err = bdb.Get(key8, k8hash)
		if err != nil {
			t.Fatal(err)
		} else if string(v) != "18.2" {
			t.Fatal(string(v))
		} else {
			if closer != nil {
				closer()
			}
		}
	}
}

func TestKVAppend(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("b")
		khash := hash.Fnv32(key)
		if err := bdb.Set(key, khash, []byte("abc")); err != nil {
			t.Fatal(err)
		}
		testCheckKeyValue(t, bdb, key, khash, []byte("abc"))

		n, err := bdb.Append(key, khash, []byte("def"))
		require.NoError(t, err)
		require.Equal(t, int64(6), n)
		testCheckKeyValue(t, bdb, key, khash, []byte("abcdef"))

		n, err = bdb.Expire(key, khash, 1)
		require.NoError(t, err)
		require.Equal(t, int64(1), n)

		time.Sleep(2 * time.Second)

		n, err = bdb.Append(key, khash, []byte("1234"))
		require.NoError(t, err)
		require.Equal(t, int64(4), n)
		testCheckKeyValue(t, bdb, key, khash, []byte("1234"))

		key1 := []byte("b1")
		khash1 := hash.Fnv32(key1)
		n, err = bdb.Append(key1, khash1, []byte("testkey1"))
		require.NoError(t, err)
		require.Equal(t, int64(8), n)
		testCheckKeyValue(t, bdb, key1, khash1, []byte("testkey1"))
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
			require.NoError(t, bdb.Set(key, khash, value1))
			require.NoError(t, bdb.SetEX(key, khash, 10, value1))
			if n, err := bdb.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n < 9 {
				t.Fatal(n)
			}
			testCheckKeyValue(t, bdb, key, khash, value1)

			if err := bdb.PSetEX(key, khash, 9200, value2); err != nil {
				t.Fatal(err)
			}
			if n, err := bdb.PTTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 9200 {
				t.Fatal(n)
			}
			if n, err := bdb.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n < 9 {
				t.Fatal(n)
			}
			testCheckKeyValue(t, bdb, key, khash, value2)

			if err := bdb.PSetEX(key, khash, 900, value2); err != nil {
				t.Fatal(err)
			}
			time.Sleep(1000 * time.Millisecond)
			testCheckKeyValue(t, bdb, key, khash, nil)
			if n, err := bdb.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != bitsdb.ErrnoKeyNotFoundOrExpire {
				t.Fatal(n)
			}
		}

		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("testdb_kv_setex_%d", i))
			checkEx(key)
		}
	}
}

func TestKVSetEX1(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key1 := []byte("testdb_kv_setex_1")
		key2 := []byte("testdb_kv_setex_2")
		value1 := []byte("hello world1")
		value2 := []byte("hello world2")
		khash1 := hash.Fnv32(key1)
		khash2 := hash.Fnv32(key2)

		v0 := extend.FormatIntToSlice(0)
		require.NoError(t, bdb.SetEX(key1, khash1, 3600, v0))
		testCheckKeyValue(t, bdb, key1, khash1, v0)

		require.NoError(t, bdb.SetEX(key1, khash1, 14000, value1))
		require.NoError(t, bdb.SetEX(key2, khash2, 14000, value2))
		testCheckKeyValue(t, bdb, key1, khash1, value1)
		testCheckKeyValue(t, bdb, key2, khash2, value2)
		require.NoError(t, bdb.bitsdb.DB.FlushVm())
		testCheckKeyValue(t, bdb, key1, khash1, value1)
		testCheckKeyValue(t, bdb, key2, khash2, value2)
	}
}

func TestKVMSetAndDel(t *testing.T) {
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
		if err := bdb.MSet(k1hash, keys...); err != nil {
			t.Fatal(err)
		}

		v, mgetClosers, err := bdb.MGet(k1hash, key1, key2, key3)
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

		n, err := bdb.Del(k1hash, key1)
		if err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		k2hash := hash.Fnv32(key2)
		err = bdb.SetEX(key2, k2hash, 1, []byte("b1"))
		if err != nil {
			t.Fatal(err)
		}

		v, mgetClosers, err = bdb.MGet(k1hash, key1, key2, key3)
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
		v, mgetClosers, err = bdb.MGet(k1hash, key1, key2, key3)
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
				n, err := bdb.SetBit(key, khash, c.offset, c.on)
				require.NoError(t, err)
				require.Equal(t, c.setexp, n)

				n, err = bdb.GetBit(key, khash, c.offset)
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
			_, err := bdb.SetBit(wkey[0:16], khash, int(n), int(n%2))
			require.NoError(t, err)
		}
	}
}

func TestKVBitCount(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("TestKVBitCount")
		khash := hash.Fnv32(key)

		n, err := bdb.BitCount(key, khash, 0, -1)
		require.NoError(t, err)
		require.Equal(t, int64(0), n)

		for i := 110; i <= 120; i++ {
			n, err = bdb.SetBit(key, khash, i, 1)
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
			{1724947200, 1725292800, 0},
		}

		t.Run("test bitcount", func(t *testing.T) {
			for _, c := range cases {
				n, err = bdb.BitCount(key, khash, c.start, c.end)
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
		n, err := bdb.BitPos(key, khash, 1, 0, -1)
		require.NoError(t, err)
		require.Equal(t, int64(-1), n)
		n, err = bdb.BitPos(key, khash, 0, 0, -1)
		require.NoError(t, err)
		require.Equal(t, int64(0), n)

		for i := 110; i <= 120; i++ {
			n, err = bdb.SetBit(key, khash, i, 1)
			require.NoError(t, err)
			require.Equal(t, int64(0), n)
		}

		n, err = bdb.SetBit(key, khash, 125, 1)
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
				n, err = bdb.BitPos(key, khash, 1, c.start, c.end)
				require.NoError(t, err)
				require.Equal(t, c.exp1, n)

				n, err = bdb.BitPos(key, khash, 0, c.start, c.end)
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
		nowTime := tclock.GetTimestampSecond()
		newTime := nowTime + 200

		checkKey := func(key, val []byte) {
			khash := hash.Fnv32(key)
			value, vcloser, e := bdb.Get(key, khash)
			require.NoError(t, e)
			require.Equal(t, val, value)
			if vcloser != nil {
				vcloser()
			}
		}

		key1 := []byte("key1")
		k1hash := hash.Fnv32(key1)
		val1 := []byte("val1")

		res, err := bdb.setNXEX(key1, k1hash, 300, val1, false)
		require.NoError(t, err)
		require.Equal(t, int64(1), res)

		res, err = bdb.setNXEX(key1, k1hash, 300, val1, false)
		require.NoError(t, err)
		require.Equal(t, int64(0), res)

		require.NoError(t, bdb.Set(key1, k1hash, val1))
		checkKey(key1, val1)
		if res, err = bdb.ExpireAt(key1, k1hash, newTime); err != nil {
			t.Fatal(err)
		} else if res != 1 {
			t.Fatal("ExpireAt key1 fail")
		}
		checkKey(key1, val1)
		if res, err = bdb.Persist(key1, k1hash); err != nil {
			t.Fatal(err)
		} else if res != 1 {
			t.Fatal("Persist fail")
		}
		checkKey(key1, val1)

		key2 := []byte("key2")
		k2hash := hash.Fnv32(key2)
		val2 := []byte("key2")
		require.NoError(t, bdb.SetEX(key2, k2hash, 1000, val2))
		checkKey(key2, val2)
		if res, err = bdb.Expire(key2, k2hash, 500); err != nil {
			t.Fatal(err)
		} else if res != 1 {
			t.Fatal("Expire key2 fail")
		}
		checkKey(key2, val2)
		require.NoError(t, bdb.SetEX(key2, k2hash, 1000, val2))
		checkKey(key2, val2)

		key3 := []byte("key3")
		k3hash := hash.Fnv32(key3)
		val3 := []byte("key3")
		if res, err = bdb.SetNXEX(key3, k3hash, 2000, val3); err != nil {
			t.Fatal(err)
		} else if res != 1 {
			t.Fatal("key3 SetNXEX fail")
		}
		checkKey(key3, val3)
		if res, err = bdb.Del(k3hash, key3); err != nil {
			t.Fatal(err)
		}
		checkKey(key3, []byte(nil))
		if err = bdb.Set(key3, k3hash, val3); err != nil {
			t.Fatal(err)
		}
		checkKey(key3, val3)
		if err = bdb.SetEX(key3, k3hash, 1000, val3); err != nil {
			t.Fatal(err)
		}
		checkKey(key3, val3)
		if res, err = bdb.Persist(key3, k3hash); err != nil {
			t.Fatal(err)
		} else if res != 1 {
			t.Fatal("Persist fail")
		}
		checkKey(key3, val3)
		if res, err = bdb.Del(k3hash, key3); err != nil {
			t.Fatal(err)
		}
		checkKey(key3, []byte(nil))
	}
}

func TestKVMissCache(t *testing.T) {
	dbPath := testCacheDBPath
	os.RemoveAll(dbPath)
	cfg := testCacheDefaultConfig()
	cfg.EnableMissCache = true
	db := testOpenBitsDb(true, dbPath, cfg)
	defer func() {
		db.Close()
		os.RemoveAll(dbPath)
		config.GlobalConfig.Plugin.OpenRaft = true
	}()

	key1 := []byte("testdb_kv_set_1")
	khash1 := hash.Fnv32(key1)
	require.NoError(t, db.Set(key1, khash1, key1))
	testCheckKeyValue(t, db, key1, khash1, key1)

	key2 := []byte("testdb_kv_set_2")
	khash2 := hash.Fnv32(key2)
	testCheckKeyValue(t, db, key2, khash2, nil)
	cv, ccloser, _, cexist := db.bitsdb.StringCache.Get(key2)
	require.Equal(t, true, cexist)
	require.Equal(t, bitsdb.MissCacheValue, cv)
	ccloser()

	require.NoError(t, db.Set(key2, khash2, key2))
	testCheckKeyValue(t, db, key2, khash2, key2)
	cv, ccloser, _, cexist = db.bitsdb.StringCache.Get(key2)
	require.Equal(t, true, cexist)
	require.Equal(t, key2, cv)
	ccloser()
}

func TestKVStringCacheCfg(t *testing.T) {
	dbPath := testCacheDBPath
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)
	cfg := dbconfig.NewConfigDefault()
	cfg.CacheSize = 10 << 20
	cfg.CacheHashSize = 10000
	db := testOpenBitsDb(true, dbPath, cfg)
	require.Equal(t, 1<<30, int(db.bitsdb.StringCache.MaxMem()))
	require.Equal(t, 1024, db.bitsdb.StringCache.Shards())
	db.Close()

	cfg.CacheSize = 200 << 20
	cfg.CacheEliminateDuration = 10
	cfg.CacheShardNum = 3
	db = testOpenBitsDb(true, dbPath, cfg)
	require.Equal(t, 1<<30, int(db.bitsdb.StringCache.MaxMem()))
	require.Equal(t, 1024, db.bitsdb.StringCache.Shards())
	db.Close()

	cfg.CacheSize = 1<<30 + 1<<20
	cfg.CacheShardNum = 1100
	db = testOpenBitsDb(true, dbPath, cfg)
	require.Equal(t, 1<<30+1<<20, int(db.bitsdb.StringCache.MaxMem()))
	require.Equal(t, 2048, db.bitsdb.StringCache.Shards())
	db.Close()
}

func TestKVStringCache(t *testing.T) {
	dbPath := testCacheDBPath
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)
	cfg := dbconfig.NewConfigDefault()
	cfg.CacheSize = 10 << 20
	cfg.CacheHashSize = 10000
	db := testOpenBitsDb(true, dbPath, cfg)

	key1 := []byte("key1")
	k1hash := hash.Fnv32(key1)
	val1 := []byte("val1")
	val2 := []byte("val2")
	duration := int64(300)
	ts := tclock.SetExpireAtMilli(duration)
	res, err := db.setNXEX(key1, k1hash, duration, val1, false)
	require.NoError(t, err)
	require.Equal(t, int64(1), res)

	v, vcloser, verr := db.Get(key1, k1hash)
	require.NoError(t, verr)
	require.Equal(t, val1, v)
	vcloser()

	val, closer, timestamp, exist := db.bitsdb.StringCache.Get(key1)
	require.Equal(t, true, exist)
	require.Equal(t, ts, int64(timestamp))
	require.Equal(t, val1, val)
	closer()

	ts = tclock.SetExpireAtMilli(duration)
	res, err = db.setNXEX(key1, k1hash, duration, val2, false)
	require.NoError(t, err)
	require.Equal(t, int64(0), res)

	val, closer, timestamp, exist = db.bitsdb.StringCache.Get(key1)
	require.Equal(t, true, exist)
	require.Equal(t, ts, int64(timestamp))
	require.Equal(t, val1, val)
	closer()

	db.Close()

}
