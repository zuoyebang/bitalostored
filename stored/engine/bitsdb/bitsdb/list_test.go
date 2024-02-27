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

package bitsdb

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/list"

	"github.com/zuoyebang/bitalostored/butils/hash"

	"github.com/stretchr/testify/require"
)

func TestListCodec(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("key")
	khash := hash.Fnv32(key)
	ek, ekCloser := base.EncodeMetaKey(key, khash)
	defer ekCloser()
	if k, err := base.DecodeMetaKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "key" {
		t.Fatal(string(k))
	}

	mkv := base.GetMkvFromPool()
	defer base.PutMkvToPool(mkv)
	lindex := mkv.GetLeftElementIndex()
	var mk [base.MetaListValueLen]byte
	base.EncodeMetaDbValueForList(mk[:], mkv)
	if err := base.DecodeMetaValueForList(mkv, mk[:]); err != nil {
		t.Fatal(err)
	} else if lindex != mkv.GetLeftElementIndex() {
		t.Log(mkv)
		t.Fatal(lindex)
	}
}

func TestListTrim(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("test_list_trim")
	khash := hash.Fnv32(key)

	initFunc := func() {
		for i := 0; i < 100; i++ {
			_, err := bdb.ListObj.RPush(key, khash, []byte(strconv.Itoa(i)))
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	initFunc()

	var err error
	err = bdb.ListObj.LTrim(key, khash, 0, 99)
	if err != nil {
		t.Fatal(err)
	}
	if l, _ := bdb.ListObj.LLen(key, khash); l != 100 {
		t.Fatal("wrong len:", l)
	}

	err = bdb.ListObj.LTrim(key, khash, 0, 50)
	if err != nil {
		t.Fatal(err)
	}
	if l, _ := bdb.ListObj.LLen(key, khash); l != 51 {
		t.Fatal("wrong len:", l)
	}

	for i := int64(0); i < 5; i++ {
		v, vcloser, err := bdb.ListObj.LIndex(key, khash, i)
		if err != nil {
			t.Fatal(err)
		}
		if string(v) != strconv.Itoa(int(i)) {
			t.Fatal("wrong value")
		}
		vcloser()
	}
	err = bdb.ListObj.LTrim(key, khash, 11, 30)
	if err != nil {
		t.Fatal(err)
	}
	if l, _ := bdb.ListObj.LLen(key, khash); l != (30 - 11 + 1) {
		t.Fatal("wrong len:", l)
	}
	for i := int64(11); i < 31; i++ {
		v, vcloser, err := bdb.ListObj.LIndex(key, khash, i-11)
		if err != nil {
			t.Fatal(err)
		}
		if string(v) != strconv.Itoa(int(i)) {
			t.Fatal("wrong value")
		}
		vcloser()
	}

	err = bdb.ListObj.LTrim(key, khash, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if l, _ := bdb.ListObj.LLen(key, khash); l != (30 - 11 + 1) {
		t.Fatal("wrong len:", l)
	}

	initFunc()
	err = bdb.ListObj.LTrim(key, khash, -3, -3)

	if err != nil {
		t.Fatal(err)
	}

	if l, _ := bdb.ListObj.LLen(key, khash); l != 1 {
		t.Fatal("wrong len:", l)
	}

	v, vcloser, err := bdb.ListObj.LIndex(key, khash, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "97" {
		t.Fatal("wrong value", string(v))
	}
	vcloser()
}

func TestDBList(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key1 := []byte("testdb_list_a1")
	khash1 := hash.Fnv32(key1)
	key2 := []byte("testdb_list_a2")
	khash2 := hash.Fnv32(key2)

	checkKeyKind := func(k []byte, h uint32, kind uint8) {
		mk, mkCloser := base.EncodeMetaKey(k, h)
		mkv, err := bdb.ListObj.GetMetaData(mk)
		mkCloser()
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, kind, mkv.Kind())
	}

	checkCmd := func(key []byte, khash uint32) {
		checkKeyKind(key, khash, base.KeyKindDefault)
		if ay, err := bdb.ListObj.LRange(key, khash, 0, -1); err != nil {
			t.Fatal(err)
		} else if len(ay) != 3 {
			t.Fatal(len(ay))
		} else {
			for i := range ay {
				if ay[i][0] != '1'+byte(i) {
					t.Fatal(string(ay[i]))
				}
			}
		}

		if v, vcloser, err := bdb.ListObj.RPop(key, khash); err != nil {
			t.Fatal(err)
		} else if string(v) != "3" {
			t.Fatal(string(v))
		} else if vcloser != nil {
			vcloser()
		}

		if v, vcloser, err := bdb.ListObj.LPop(key, khash); err != nil {
			t.Fatal(err)
		} else if string(v) != "1" {
			t.Fatal(string(v))
		} else if vcloser != nil {
			vcloser()
		}

		if llen, err := bdb.ListObj.LLen(key, khash); err != nil {
			t.Fatal(err)
		} else if llen != 1 {
			t.Fatal(llen)
		}
	}

	if n, err := bdb.ListObj.RPush(key1, khash1, []byte("1"), []byte("2"), []byte("3")); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}
	checkCmd(key1, khash1)

	if n, err := bdb.ListObj.RPush(key2, khash2, []byte("1"), []byte("2"), []byte("3")); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}
	checkCmd(key2, khash2)

}

func TestDBListLrem(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("testdb_list_a_lrem")
	khash := hash.Fnv32(key)

	if n, err := bdb.ListObj.RPush(key, khash, []byte("1"), []byte("2"), []byte("3"), []byte("4")); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if ay, err := bdb.ListObj.LRange(key, khash, 0, -1); err != nil {
		t.Fatal(err)
	} else if len(ay) != 4 {
		t.Fatal(len(ay))
	} else {
		for i := range ay {
			if ay[i][0] != '1'+byte(i) {
				t.Fatal(string(ay[i]))
			}
		}
	}

	if k, vcloser, err := bdb.ListObj.RPop(key, khash); err != nil {
		t.Fatal(err)
	} else if string(k) != "4" {
		t.Fatal(string(k))
	} else if vcloser != nil {
		vcloser()
	}

	if k, vcloser, err := bdb.ListObj.LPop(key, khash); err != nil {
		t.Fatal(err)
	} else if string(k) != "1" {
		t.Fatal(string(k))
	} else if vcloser != nil {
		vcloser()
	}

	if llen, err := bdb.ListObj.LLen(key, khash); err != nil {
		t.Fatal(err)
	} else if llen != 2 {
		t.Fatal(llen)
	}
}

func TestListExists(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("lkeyexists_test")
	khash := hash.Fnv32(key)

	if n, err := bdb.StringObj.Exists(key, khash); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal("invalid value ", n)
	}
	bdb.ListObj.LPush(key, khash, []byte("hello"), []byte("world"))
	if n, err := bdb.StringObj.Exists(key, khash); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal("invalid value ", n)
	}
}

func TestListPop(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("lpop_test")
	khash := hash.Fnv32(key)

	if v, vcloser, err := bdb.ListObj.LPop(key, khash); err != nil {
		t.Fatal(err)
	} else if v != nil {
		t.Fatal(v)
	} else if vcloser != nil {
		vcloser()
	}

	if s, err := bdb.ListObj.LLen(key, khash); err != nil {
		t.Fatal(err)
	} else if s != 0 {
		t.Fatal(s)
	}

	for i := 0; i < 10; i++ {
		if n, err := bdb.ListObj.LPush(key, khash, []byte("a")); err != nil {
			t.Fatal(err)
		} else if n != int64(1+i) {
			t.Fatal(n)
		}
	}

	if s, err := bdb.ListObj.LLen(key, khash); err != nil {
		t.Fatal(err)
	} else if s != 10 {
		t.Fatal(s)
	}

	for i := 0; i < 10; i++ {
		if _, vcloser, err := bdb.ListObj.LPop(key, khash); err != nil {
			t.Fatal(err)
		} else if vcloser != nil {
			vcloser()
		}
	}

	if s, err := bdb.ListObj.LLen(key, khash); err != nil {
		t.Fatal(err)
	} else if s != 0 {
		t.Fatal(s)
	}

	if v, vcloser, err := bdb.ListObj.LPop(key, khash); err != nil {
		t.Fatal(err)
	} else if v != nil {
		t.Fatal(v)
	} else if vcloser != nil {
		vcloser()
	}
}

func TestRpushFull(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("lkey_rpush_full_test")
	khash := hash.Fnv32(key)
	bdb.ListObj.ListPush(key, khash, false, false, []byte("a"))

	setListIndex := func(key []byte, leftIndex, rightIndex uint32) {
		mk, mkCloser := base.EncodeMetaKey(key, hash.Fnv32(key))
		defer mkCloser()
		mkv, err := bdb.ListObj.GetMetaData(mk)
		require.Equal(t, nil, err)
		defer base.PutMkvToPool(mkv)
		mkv.SetLeftIndex(leftIndex)
		mkv.SetRightIndex(rightIndex)
		err = bdb.ListObj.SetMetaData(mk, mkv)
		require.Equal(t, nil, err)
	}

	setListIndex(key, base.MinIndex, base.MaxIndex)
	if n, err := bdb.ListObj.RPush(key, khash, []byte("abc")); err == nil {
		t.Fatal("list is full, rpush not return error")
	} else {
		if err != list.ErrWriteNoSpace {
			t.Fatal(err)
		}
		require.Equal(t, err, list.ErrWriteNoSpace)
		require.Equal(t, int64(0), n)
	}

	setListIndex(key, 100, 99)
	if n, err := bdb.ListObj.RPush(key, khash, []byte("abc")); err == nil {
		t.Fatal("list is full, rpush not return error")
	} else {
		if err != list.ErrWriteNoSpace {
			t.Fatal(err)
		}
		require.Equal(t, err, list.ErrWriteNoSpace)
		require.Equal(t, int64(0), n)
	}
}

func TestLrange(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("lrange_bound_test")
	khash := hash.Fnv32(key)
	push(bdb.ListObj, key, khash, base.MaxIndex, base.MinIndex, []byte("init"))

	n, err := bdb.ListObj.RPush(key, khash, []byte("bcd"))
	require.Equal(t, int64(2), n)
	require.Equal(t, nil, err)

	n, err = bdb.ListObj.LPush(key, khash, []byte("abc"))
	require.Equal(t, int64(3), n)
	require.Equal(t, nil, err)

	r, err := bdb.ListObj.LRange(key, khash, 0, -1)
	require.Equal(t, 3, len(r))
	require.Equal(t, []byte("abc"), r[0])
	require.Equal(t, []byte("bcd"), r[2])

	key1 := []byte("flowrt2:stasticNode:t1686192660000_d150003_n150002_ni0")
	khash1 := hash.Fnv32(key1)
	for i := 0; i < 100; i++ {
		n, err = bdb.ListObj.LPush(key1, khash1, []byte(fmt.Sprintf("value%d", i)))
		require.Equal(t, int64(i+1), n)
	}
	r1, err1 := bdb.ListObj.LRange(key1, khash1, 0, -1)
	require.Equal(t, nil, err1)
	require.Equal(t, 100, len(r1))

	for i := 100; i < 200; i++ {
		n, err = bdb.ListObj.LPush(key1, khash1, []byte(fmt.Sprintf("value%d", i)))
		require.Equal(t, int64(i+1), n)
	}

	bdb.FlushAllDB()
	r1, err1 = bdb.ListObj.LRange(key1, khash1, 0, -1)
	require.Equal(t, nil, err1)
	require.Equal(t, 200, len(r1))
}

func TestLset(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("lrange_lset_test")
	khash := hash.Fnv32(key)

	err := bdb.ListObj.LSet(key, khash, 0, []byte("a"))
	require.Equal(t, list.ErrNoSuchKey, err)

	values := []string{"a", "b", "c", "d"}
	for _, v := range values {
		_, err = bdb.ListObj.LPush(key, khash, []byte(v))
		require.Equal(t, nil, err)
	}

	err = bdb.ListObj.LSet(key, khash, 0, []byte("a0"))
	require.Equal(t, nil, err)
	value, vcloser, err := bdb.ListObj.LIndex(key, khash, 0)
	require.Equal(t, []byte("a0"), value)
	require.Equal(t, nil, err)
	vcloser()

	checkList := make(map[int]error, 0)
	checkList[0] = nil
	checkList[3] = nil
	checkList[4] = list.ErrIndexOutOfRange
	checkList[-1] = nil
	checkList[-4] = nil
	checkList[-5] = list.ErrIndexOutOfRange

	for index, expectErr := range checkList {
		err = bdb.ListObj.LSet(key, khash, int64(index), []byte(""))
		require.Equal(t, expectErr, err)
	}

	bdb.ListObj.Del(khash, key)
}

func TestLInsert(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("linsert_leftindex<rightindex")
	khash := hash.Fnv32(key)

	for i := 20; i >= 0; i-- {
		bdb.ListObj.LPush(key, khash, getValue(i))
	}
	beforeInsert := []byte("before-insert")
	n, err := bdb.ListObj.LInsert(key, khash, false, getValue(1), beforeInsert)
	require.Equal(t, int64(22), n)
	require.Equal(t, err, nil)
	l, _ := bdb.ListObj.LRange(key, khash, 0, -1)
	require.Equal(t, 22, len(l))
	require.Equal(t, beforeInsert, l[2])

	afterInsert := []byte("after-insert")
	n, err = bdb.ListObj.LInsert(key, khash, true, getValue(1), afterInsert)
	require.Equal(t, int64(23), n)
	require.Equal(t, err, nil)
	l, _ = bdb.ListObj.LRange(key, khash, 0, -1)
	require.Equal(t, 23, len(l))
	require.Equal(t, afterInsert, l[1])

	n, err = bdb.ListObj.LInsert(key, khash, true, getValue(15), beforeInsert)
	require.Equal(t, int64(24), n)
	require.Equal(t, err, nil)
	l, _ = bdb.ListObj.LRange(key, khash, 0, -1)
	require.Equal(t, 24, len(l))
	require.Equal(t, beforeInsert, l[len(l)-7])
}

func TestLInsert2(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key2 := []byte("linsert_leftindex>rightindex")
	k2hash := hash.Fnv32(key2)
	beforeInsert := []byte("beforeInsert")
	afterInsert := []byte("afterInsert")
	push(bdb.ListObj, key2, k2hash, base.MaxIndex-5, base.MaxIndex-4, []byte("init"))
	for i := 0; i <= 20; i++ {
		bdb.ListObj.RPush(key2, k2hash, getValue(i))
	}
	n, err := bdb.ListObj.LInsert(key2, k2hash, true, getValue(1), beforeInsert)
	require.Equal(t, int64(23), n)
	require.Equal(t, err, nil)
	l, _ := bdb.ListObj.LRange(key2, k2hash, 0, -1)
	require.Equal(t, 23, len(l))

	require.Equal(t, beforeInsert, l[2])

	n, err = bdb.ListObj.LInsert(key2, k2hash, false, getValue(1), afterInsert)
	require.Equal(t, int64(24), n)
	require.Equal(t, err, nil)
	l, _ = bdb.ListObj.LRange(key2, k2hash, 0, -1)
	require.Equal(t, 24, len(l))

	require.Equal(t, afterInsert, l[4])

	n, err = bdb.ListObj.LInsert(key2, k2hash, true, getValue(17), beforeInsert)

	require.Equal(t, int64(25), n)
	require.Equal(t, err, nil)
	l, _ = bdb.ListObj.LRange(key2, k2hash, 0, -1)
	require.Equal(t, 25, len(l))

	require.Equal(t, beforeInsert, l[len(l)-5])
}

func TestLRem1(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("lrem_leftindex<rightindex")
	khash := hash.Fnv32(key)

	for i := 0; i < 2; i++ {
		for j := 19; j >= 0; j-- {
			bdb.ListObj.LPush(key, khash, getValue(j))
		}
	}
	n, _ := bdb.ListObj.LRem(key, khash, 0, getValue(1))
	require.Equal(t, int64(2), n)

	l, _ := bdb.ListObj.LRange(key, khash, 0, -1)
	require.Equal(t, 38, len(l))

	n, _ = bdb.ListObj.LRem(key, khash, 0, getValue(17))
	require.Equal(t, int64(2), n)

	l, _ = bdb.ListObj.LRange(key, khash, 0, -1)
	require.Equal(t, 36, len(l))
}

func TestLRem2(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("lrem_rightindex<leftindex")
	khash := hash.Fnv32(key)

	push(bdb.ListObj, key, khash, base.MaxIndex-5, base.MaxIndex-4, []byte("init"))

	for i := 0; i < 2; i++ {
		for j := 19; j >= 0; j-- {
			bdb.ListObj.RPush(key, khash, getValue(j))
		}
	}
	n, _ := bdb.ListObj.LRem(key, khash, 0, getValue(1))
	require.Equal(t, int64(2), n)

	l, _ := bdb.ListObj.LRange(key, khash, 0, -1)
	require.Equal(t, 39, len(l))

	n, _ = bdb.ListObj.LRem(key, khash, 0, getValue(17))
	require.Equal(t, int64(2), n)

	l, _ = bdb.ListObj.LRange(key, khash, 0, -1)
	require.Equal(t, 37, len(l))
}

func TestListRpushLimit(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("rpush_limit")
	khash := hash.Fnv32(key)

	for i := 0; i < 10; i++ {
		if n, err := bdb.ListObj.RPush(key, khash, []byte("a")); err != nil {
			t.Fatal(err)
		} else if n != int64(1+i) {
			t.Fatal(n)
		}
	}

	if s, err := bdb.ListObj.LLen(key, khash); err != nil {
		t.Fatal(err)
	} else if s != 10 {
		t.Fatal(s)
	}

	for i := 0; i < 10; i++ {
		if _, vcloser, err := bdb.ListObj.LPop(key, khash); err != nil {
			t.Fatal(err)
		} else if vcloser != nil {
			vcloser()
		}
	}

	if s, err := bdb.ListObj.LLen(key, khash); err != nil {
		t.Fatal(err)
	} else if s != 0 {
		t.Fatal(s)
	}
}

func TestListLpushLimit(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("lpush_limit")
	khash := hash.Fnv32(key)

	for i := 0; i < 10; i++ {
		if n, err := bdb.ListObj.LPush(key, khash, []byte("a"+strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		} else if n != int64(1+i) {
			t.Fatal(n)
		}
	}

	if s, err := bdb.ListObj.LLen(key, khash); err != nil {
		t.Fatal(err)
	} else if s != 10 {
		t.Fatal(s)
	}

	for i := 0; i < 10; i++ {
		if _, vcloser, err := bdb.ListObj.RPop(key, khash); err != nil {
			t.Fatal(err)
		} else if vcloser != nil {
			vcloser()
		}
	}

	if s, err := bdb.ListObj.LLen(key, khash); err != nil {
		t.Fatal(err)
	} else if s != 0 {
		t.Fatal(s)
	}
}

func getValue(i int) []byte {
	return []byte("t" + strconv.Itoa(i))
}

func push(lo *list.ListObject, key []byte, khash uint32, leftIndex, rightIndex uint32, args ...[]byte) (int64, error) {
	unlockKey := lo.LockKey(khash)
	defer unlockKey()

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := lo.GetMetaData(mk)
	if err != nil {
		return 0, err
	}
	defer base.PutMkvToPool(mkv)

	if !mkv.IsAlive() {
		mkv.Reset(lo.GetNextKeyId())
	}

	mkv.SetLeftIndex(leftIndex)
	mkv.SetRightIndex(rightIndex)

	wb := lo.GetDataWriteBatchFromPool()
	defer lo.PutWriteBatchToPool(wb)

	var index []byte
	keyVersion := mkv.Version()
	for _, v := range args {
		index = mkv.GetRightIndexByte()
		ekf, ekfCloser := base.EncodeDataKey(keyVersion, khash, index)
		_ = wb.Put(ekf, v)
		ekfCloser()
		mkv.ModifyRightIndex(1)
		mkv.IncrSize(1)
	}

	if err = wb.Commit(); err != nil {
		return 0, err
	}

	if err = lo.SetMetaData(mk, mkv); err != nil {
		return 0, err
	}

	return mkv.Size(), nil
}
