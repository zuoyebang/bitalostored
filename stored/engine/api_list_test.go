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
	"fmt"
	"strconv"
	"testing"

	"github.com/zuoyebang/bitalostored/stored/internal/errn"

	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/stretchr/testify/require"
)

func TestListTrim(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	key := []byte("test_list_trim")
	khash := hash.Fnv32(key)

	initFunc := func() {
		for i := 0; i < 100; i++ {
			_, err := bdb.db.RPush(key, khash, []byte(strconv.Itoa(i)))
			require.NoError(t, err)
		}
	}

	initFunc()

	require.NoError(t, bdb.db.LTrim(key, khash, 0, 99))
	if l, _ := bdb.db.LLen(key, khash); l != 100 {
		t.Fatal("wrong len:", l)
	}

	require.NoError(t, bdb.db.LTrim(key, khash, 0, 50))
	if l, _ := bdb.db.LLen(key, khash); l != 51 {
		t.Fatal("wrong len:", l)
	}

	for i := int64(0); i < 5; i++ {
		v, vcloser, err := bdb.db.LIndex(key, khash, i)
		if err != nil {
			t.Fatal(err)
		}
		if string(v) != strconv.Itoa(int(i)) {
			t.Fatal("wrong value")
		}
		vcloser()
	}

	require.NoError(t, bdb.db.LTrim(key, khash, 11, 30))
	if l, _ := bdb.db.LLen(key, khash); l != (30 - 11 + 1) {
		t.Fatal("wrong len:", l)
	}

	for i := int64(11); i < 31; i++ {
		v, vcloser, err := bdb.db.LIndex(key, khash, i-11)
		if err != nil {
			t.Fatal(err)
		}
		if string(v) != strconv.Itoa(int(i)) {
			t.Fatal("wrong value")
		}
		vcloser()
	}

	require.NoError(t, bdb.db.LTrim(key, khash, 0, -1))
	if l, _ := bdb.db.LLen(key, khash); l != (30 - 11 + 1) {
		t.Fatal("wrong len:", l)
	}

	initFunc()
	require.NoError(t, bdb.db.LTrim(key, khash, -3, -3))
	if l, _ := bdb.db.LLen(key, khash); l != 1 {
		t.Fatal("wrong len:", l)
	}

	v, vcloser, err := bdb.db.LIndex(key, khash, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "97" {
		t.Fatal("wrong value", string(v))
	}
	vcloser()
}

func TestDBList(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()
	key1 := []byte("testdb_list_a1")
	khash1 := hash.Fnv32(key1)
	key2 := []byte("testdb_list_a2")
	khash2 := hash.Fnv32(key2)

	checkCmd := func(key []byte, khash uint32) {
		res, closer, err := bdb.db.LRange(key, khash, 0, -1)
		require.NoError(t, err)
		require.Equal(t, 3, len(res))
		for i := range res {
			require.Equal(t, '1'+byte(i), res[i][0])
		}
		closer()

		v1, vcloser1, err1 := bdb.db.RPop(key, khash)
		require.NoError(t, err1)
		require.Equal(t, []byte("3"), v1)
		vcloser1()

		v2, vcloser2, err2 := bdb.db.LPop(key, khash)
		require.NoError(t, err2)
		require.Equal(t, []byte("1"), v2)
		vcloser2()

		llen, err3 := bdb.db.LLen(key, khash)
		require.NoError(t, err3)
		require.Equal(t, int64(1), llen)
	}

	if n, err := bdb.db.RPush(key1, khash1, []byte("1"), []byte("2"), []byte("3")); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}
	checkCmd(key1, khash1)

	if n, err := bdb.db.RPush(key2, khash2, []byte("1"), []byte("2"), []byte("3")); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}
	checkCmd(key2, khash2)
}

func TestListExists(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	key := []byte("lkeyexists_test")
	khash := hash.Fnv32(key)

	if n, err := bdb.db.Exists(key, khash); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal("invalid value ", n)
	}
	bdb.db.LPush(key, khash, []byte("hello"), []byte("world"))
	if n, err := bdb.db.Exists(key, khash); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal("invalid value ", n)
	}
}

func TestListPop(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	key := []byte("lpop_test")
	khash := hash.Fnv32(key)

	if v, vcloser, err := bdb.db.LPop(key, khash); err != nil {
		t.Fatal(err)
	} else if v != nil {
		t.Fatal(v)
	} else if vcloser != nil {
		vcloser()
	}

	if s, err := bdb.db.LLen(key, khash); err != nil {
		t.Fatal(err)
	} else if s != 0 {
		t.Fatal(s)
	}

	for i := 0; i < 10; i++ {
		if n, err := bdb.db.LPush(key, khash, []byte("a")); err != nil {
			t.Fatal(err)
		} else if n != int64(1+i) {
			t.Fatal(n)
		}
	}

	if s, err := bdb.db.LLen(key, khash); err != nil {
		t.Fatal(err)
	} else if s != 10 {
		t.Fatal(s)
	}

	for i := 0; i < 10; i++ {
		if _, vcloser, err := bdb.db.LPop(key, khash); err != nil {
			t.Fatal(err)
		} else if vcloser != nil {
			vcloser()
		}
	}

	if s, err := bdb.db.LLen(key, khash); err != nil {
		t.Fatal(err)
	} else if s != 0 {
		t.Fatal(s)
	}

	if v, vcloser, err := bdb.db.LPop(key, khash); err != nil {
		t.Fatal(err)
	} else if v != nil {
		t.Fatal(v)
	} else if vcloser != nil {
		vcloser()
	}
}

func TestLrange(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	key := []byte("lrange_bound_test")
	khash := hash.Fnv32(key)

	n, err := bdb.db.RPush(key, khash, []byte("init"))
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, err = bdb.db.RPush(key, khash, []byte("bcd"))
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	n, err = bdb.db.LPush(key, khash, []byte("abc"))
	require.NoError(t, err)
	require.Equal(t, int64(3), n)

	r, rcloser, err := bdb.db.LRange(key, khash, 0, -1)
	require.NoError(t, err)
	require.Equal(t, 3, len(r))
	require.Equal(t, []byte("abc"), r[0])
	require.Equal(t, []byte("bcd"), r[2])
	rcloser()

	key1 := []byte("flowrt2:stasticNode:t1686192660000_d150003_n150002_ni0")
	khash1 := hash.Fnv32(key1)
	for i := 0; i < 100; i++ {
		n, err = bdb.db.LPush(key1, khash1, []byte(fmt.Sprintf("value%d", i)))
		require.Equal(t, int64(i+1), n)
	}
	r1, r1Closer, err1 := bdb.db.LRange(key1, khash1, 0, -1)
	require.NoError(t, err1)
	require.Equal(t, 100, len(r1))
	r1Closer()

	for i := 100; i < 200; i++ {
		n, err = bdb.db.LPush(key1, khash1, []byte(fmt.Sprintf("value%d", i)))
		require.Equal(t, int64(i+1), n)
	}

	r1, r1Closer, err1 = bdb.db.LRange(key1, khash1, 0, -1)
	require.NoError(t, err1)
	require.Equal(t, 200, len(r1))
	r1Closer()
}

func TestLset(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	key := []byte("lrange_lset_test")
	khash := hash.Fnv32(key)

	err := bdb.db.LSet(key, khash, 0, []byte("a"))
	if err.Error() != errn.ErrNoSuchKey.Error() {
		t.Fatal(err)
	}

	values := []string{"a", "b", "c", "d"}
	for _, v := range values {
		_, err = bdb.db.LPush(key, khash, []byte(v))
		require.Equal(t, nil, err)
	}

	err = bdb.db.LSet(key, khash, 0, []byte("a0"))
	require.Equal(t, nil, err)
	value, vcloser, err := bdb.db.LIndex(key, khash, 0)
	require.Equal(t, []byte("a0"), value)
	require.Equal(t, nil, err)
	vcloser()

	checkList := make(map[int]error, 0)
	checkList[0] = nil
	checkList[3] = nil
	checkList[4] = errn.ErrIndexOutOfRange
	checkList[-1] = nil
	checkList[-4] = nil
	checkList[-5] = errn.ErrIndexOutOfRange

	for index, expectErr := range checkList {
		err = bdb.db.LSet(key, khash, int64(index), []byte(""))
		if expectErr == nil {
			if err != nil {
				t.Fatal(err)
			}
		} else if err.Error() != expectErr.Error() {
			t.Fatal(err)
		}
	}

	bdb.db.Del(khash, key)
}

func TestListRpush(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	key := []byte("rpush_limit")
	khash := hash.Fnv32(key)

	for i := 0; i < 10; i++ {
		if n, err := bdb.db.RPush(key, khash, []byte("a")); err != nil {
			t.Fatal(err)
		} else if n != int64(1+i) {
			t.Fatal(n)
		}
	}

	if s, err := bdb.db.LLen(key, khash); err != nil {
		t.Fatal(err)
	} else if s != 10 {
		t.Fatal(s)
	}

	for i := 0; i < 10; i++ {
		if _, vcloser, err := bdb.db.LPop(key, khash); err != nil {
			t.Fatal(err)
		} else if vcloser != nil {
			vcloser()
		}
	}

	if s, err := bdb.db.LLen(key, khash); err != nil {
		t.Fatal(err)
	} else if s != 0 {
		t.Fatal(s)
	}
}

func TestListLpush(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	key := []byte("lpush_limit")
	khash := hash.Fnv32(key)

	for i := 0; i < 10; i++ {
		if n, err := bdb.db.LPush(key, khash, []byte("a"+strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		} else if n != int64(1+i) {
			t.Fatal(n)
		}
	}

	if s, err := bdb.db.LLen(key, khash); err != nil {
		t.Fatal(err)
	} else if s != 10 {
		t.Fatal(s)
	}

	for i := 0; i < 10; i++ {
		if _, vcloser, err := bdb.db.RPop(key, khash); err != nil {
			t.Fatal(err)
		} else if vcloser != nil {
			vcloser()
		}
	}

	if s, err := bdb.db.LLen(key, khash); err != nil {
		t.Fatal(err)
	} else if s != 0 {
		t.Fatal(s)
	}
}
