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
	"strconv"
	"testing"
	"time"

	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/stretchr/testify/require"
)

func TestHsetEmptyValueAndCheckLen(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()
	key := []byte("hash_empty_key")
	khash := hash.Fnv32(key)
	field1 := []byte("f1")
	field2 := []byte("f2")
	var emptyValue []byte
	value := []byte("value")

	hsetTwice := func(key []byte, field []byte, first []byte, second []byte) {
		if n, err := bdb.db.HSet(key, khash, field, first); err != nil {
			t.Fatalf("HSet key(%s) err(%s)", string(key), err)
		} else if n != 1 {
			t.Fatalf("HSet key(%s) n err exp(1) act(%d)", string(key), n)
		}
		if n, err := bdb.db.HLen(key, khash); err != nil {
			t.Fatalf("HLen key(%s) err(%s)", string(key), err)
		} else if n != 1 {
			t.Fatalf("HLen key(%s) n err exp(1) act(%d)", string(key), n)
		}
		v, vCloser, err := bdb.db.HGet(key, khash, field)
		if err != nil {
			t.Fatalf("HGet key(%s) err(%s)", string(key), err)
		} else if !bytes.Equal(v, first) {
			t.Fatalf("HGet value not equal exp:%s act:%s ", string(first), string(v))
		}
		vCloser()

		if n, err := bdb.db.HSet(key, khash, field, second); err != nil {
			t.Fatalf("HSet2 key(%s) err(%s)", string(key), err)
		} else if n != 0 {
			t.Fatalf("HSet2 key(%s) n err exp(0) act(%d)", string(key), n)
		}
		if n, err := bdb.db.HLen(key, khash); err != nil {
			t.Fatalf("HLen2 key(%s) err(%s)", string(key), err)
		} else if n != 1 {
			t.Fatalf("HLen2 key(%s) n err exp(1) act(%d)", string(key), n)
		}
		v, vCloser, err = bdb.db.HGet(key, khash, field)
		if err != nil {
			t.Fatalf("HGet2 key(%s) err(%s)", string(key), err)
		} else if !bytes.Equal(v, second) {
			t.Fatalf("HGet2 value not equal exp:%s act:%s ", string(first), string(v))
		}
		vCloser()

		n, err := bdb.db.Del(khash, key)
		if err != nil {
			t.Fatalf("Del key(%s) err(%s)", string(key), err)
		} else if n != 1 {
			t.Fatalf("Del key(%s) n err exp(1) act(%d)", string(key), n)
		}
	}
	hsetTwice(key, field1, emptyValue, value)
	hsetTwice(key, field2, value, emptyValue)
}

func TestHmsetEmptyValueAndCheckLen(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	key := []byte("hmset_empty_key")
	khash := hash.Fnv32(key)
	field1 := []byte("f1")
	field2 := []byte("f2")
	var emptyValue []byte
	value := []byte("value")
	hmsetFunc := func(key []byte, field1 []byte, field2 []byte, first []byte, second []byte) {
		if err := bdb.db.HMset(key, khash, field1, first, field2, second); err != nil {
			t.Fatal(err)
		}

		if n, err := bdb.db.HLen(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}
	}

	hmsetFunc(key, field1, field2, emptyValue, value)
	hmsetFunc(key, field1, field2, value, emptyValue)
	n, err := bdb.db.Del(khash, key)
	if err != nil {
		t.Fatalf("Del key(%s) err(%s)", string(key), err)
	} else if n != 1 {
		t.Fatalf("Del key(%s) n err exp(1) act(%d)", string(key), n)
	}
}

func TestHashDel(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		khash := hash.Fnv32(key)
		if n, err := bdb.db.HSet(key, khash, []byte("a"), []byte("hello world 1")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.db.Del(khash, key); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.db.HLen(key, hash.Fnv32(key)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}
}

func TestHashHSet_HGet_HDEL_HLEN(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()
	key1 := []byte("testhash1")
	khash1 := hash.Fnv32(key1)
	if n, err := bdb.db.HSet(key1, khash1, []byte("h1"), []byte("v1")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	data1, vCloser1, err1 := bdb.db.HGet(key1, khash1, []byte("h1"))
	if err1 != nil {
		t.Fatal(err1)
	}
	if string(data1) != "v1" {
		t.Fatal(string(data1))
	}
	vCloser1()
	if n, err := bdb.db.HSet(key1, khash1, []byte("h1"), []byte("v1")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
	if n, err := bdb.db.HSet(key1, khash1, []byte("h1"), []byte("v11")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	for i := 0; i < 10; i++ {
		key := []byte("hash_hset_hget_hdel_test")
		khash := hash.Fnv32(key)
		if n, err := bdb.db.HSet(key, khash, []byte("a"), []byte("hello world 1")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		data, vCloser, err := bdb.db.HGet(key, khash, []byte("a"))
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "hello world 1" {
			t.Fatal(string(data))
		}
		vCloser()

		if n, err := bdb.db.HSet(key, khash, []byte("a"), []byte("hello world 1")); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
		if n, err := bdb.db.HSet(key, khash, []byte("a"), []byte("hello world 11")); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if n, err := bdb.db.HSet(key, khash, []byte("b"), []byte("hello world 2")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := bdb.db.HLen(key, hash.Fnv32(key)); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		if n, err := bdb.db.HDel(key, khash, []byte("a")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := bdb.db.HDel(key, khash, []byte("a")); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if n, err := bdb.db.HLen(key, hash.Fnv32(key)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.db.Del(khash, key); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.db.HLen(key, hash.Fnv32(key)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}
}

func TestHashRead(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()
	key := []byte("hash_hmset_hmget_hkeys_hgetall")
	khash := hash.Fnv32(key)
	args := make([][]byte, 0, 4)
	args = append(args, []byte("a"), []byte("hello world 1"))
	args = append(args, []byte("b"), []byte("hello world 2"))

	require.NoError(t, bdb.db.HMset(key, khash, args...))
	require.NoError(t, bdb.db.HMset(key, khash, args...))

	ay, vClosers, err := bdb.db.HMget(key, hash.Fnv32(key), []byte("a"), []byte("b"))
	require.NoError(t, err)
	require.Equal(t, []byte("hello world 1"), ay[0])
	require.Equal(t, []byte("hello world 2"), ay[1])
	for i := range vClosers {
		vClosers[i]()
	}

	ay, vClosers, err = bdb.db.HMget(key, hash.Fnv32(key), []byte("a"), []byte("c"))
	require.NoError(t, err)
	require.Equal(t, []byte("hello world 1"), ay[0])
	require.Equal(t, []byte(nil), ay[1])
	for i := range vClosers {
		vClosers[i]()
	}

	ay, vClosers, err = bdb.db.HMget(key, hash.Fnv32(key), []byte("c"), []byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte(nil), ay[0])
	require.Equal(t, []byte("hello world 1"), ay[1])
	for i := range vClosers {
		vClosers[i]()
	}

	resHgetall, resHgetallCloser, err1 := bdb.db.HGetAll(key, khash)
	require.NoError(t, err1)
	require.Equal(t, 4, len(resHgetall))
	require.Equal(t, []byte("a"), resHgetall[0])
	require.Equal(t, []byte("hello world 1"), resHgetall[1])
	require.Equal(t, []byte("b"), resHgetall[2])
	require.Equal(t, []byte("hello world 2"), resHgetall[3])
	resHgetallCloser()

	resHkeys, closer, err2 := bdb.db.HKeys(key, khash)
	require.NoError(t, err2)
	require.Equal(t, 2, len(resHkeys))
	require.Equal(t, []byte("a"), resHkeys[0])
	require.Equal(t, []byte("b"), resHkeys[1])
	closer()

	resHValues, resHValuesCloser, err3 := bdb.db.HValues(key, khash)
	require.NoError(t, err3)
	require.Equal(t, 2, len(resHValues))
	require.Equal(t, []byte("hello world 1"), resHValues[0])
	require.Equal(t, []byte("hello world 2"), resHValues[1])
	resHValuesCloser()
}

func TestHashTTL(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()
	key := []byte("hash_ttl_test")
	khash := hash.Fnv32(key)
	field := []byte("field")
	val := []byte("aa")
	if n, err := bdb.db.HSet(key, khash, field, val); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	data, vCloser, err := bdb.db.HGet(key, khash, field)
	if err != nil {
		t.Fatal(err)
	} else if string(data) != string(val) {
		t.Fatal(string(data))
	}
	vCloser()

	if n, err := bdb.db.Expire(key, khash, 4); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if ttl, err := bdb.db.TTL(key, khash); err != nil {
		t.Fatal(err)
	} else if ttl != 4 {
		t.Fatal(ttl)
	}

	data, vCloser, err = bdb.db.HGet(key, khash, field)
	if err != nil {
		t.Fatal(err)
	} else if string(data) != string(val) {
		t.Fatal(string(data))
	}
	vCloser()

	time.Sleep(4 * time.Second)

	if n, err := bdb.db.Exists(key, khash); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
}

func TestHashExists(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()
	key := []byte("hash_exists_test_a")
	khash := hash.Fnv32(key)

	v, err := bdb.db.Exists(key, khash)
	if err != nil {
		t.Fatal(err.Error())
	} else if v != 0 {
		t.Fatal("invalid value ", v)
	}

	if _, err := bdb.db.HSet(key, khash, []byte("hello"), []byte("world")); err != nil {
		t.Fatal(err.Error())
	}

	v, err = bdb.db.Exists(key, khash)
	if err != nil {
		t.Fatal(err.Error())
	}
	if v != 1 {
		t.Fatal("invalid value ", v)
	}
}

func TestHashHIncrBy(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	key1 := []byte("hash_hincrby_test1")
	khash1 := hash.Fnv32(key1)
	field1 := testRandBytes(100)
	cnt := int64(0)
	for i := int64(1); i <= int64(10); i++ {
		n, err := bdb.db.HIncrBy(key1, khash1, field1, i)
		require.NoError(t, err)
		cnt += i
		require.Equal(t, cnt, n)
	}
	data, vCloser, err := bdb.db.HGet(key1, khash1, field1)
	require.NoError(t, err)
	v, _ := strconv.ParseInt(unsafe2.String(data), 10, 64)
	require.Equal(t, v, cnt)
	vCloser()

	key2 := []byte("hash_hincrby_test2")
	khash2 := hash.Fnv32(key2)
	field2 := testRandBytes(100)
	cnt = int64(0)
	for i := int64(1); i <= int64(10); i++ {
		n, err := bdb.db.HIncrBy(key2, khash2, field2, i)
		require.NoError(t, err)
		cnt += i
		require.Equal(t, cnt, n)
	}
	data, vCloser, err = bdb.db.HGet(key2, khash2, field2)
	require.NoError(t, err)
	v, _ = strconv.ParseInt(unsafe2.String(data), 10, 64)
	require.Equal(t, v, cnt)
	vCloser()
}

func TestHashHMSet(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()
	key := []byte("hash_hmset_1")
	khash := hash.Fnv32(key)
	args := make([][]byte, 0, 6)
	args = append(args, []byte("online"), []byte("0"))
	args = append(args, []byte("idconline"), []byte("2:0"))
	args = append(args, []byte("stream"), []byte("0"))
	require.NoError(t, bdb.db.HMset(key, khash, args...))

	args = args[:0]
	args = append(args, []byte("online"), []byte("1"))
	args = append(args, []byte("idconline"), []byte("2:0"))
	args = append(args, []byte("stream"), []byte("0"))
	require.NoError(t, bdb.db.HMset(key, khash, args...))

	res, closer, err1 := bdb.db.HGetAll(key, khash)
	require.NoError(t, err1)
	require.Equal(t, 6, len(res))
	require.Equal(t, []byte("idconline"), res[0])
	require.Equal(t, []byte("2:0"), res[1])
	require.Equal(t, []byte("online"), res[2])
	require.Equal(t, []byte("1"), res[3])
	require.Equal(t, []byte("stream"), res[4])
	require.Equal(t, []byte("0"), res[5])
	closer()
}
