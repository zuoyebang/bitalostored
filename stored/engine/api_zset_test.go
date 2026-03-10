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
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zuoyebang/bitalostored/stored/engine/btools"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/butils/numeric"
	"github.com/stretchr/testify/require"
)

func getFloatByte(f float64) []byte {
	return numeric.Float64ToByteSort(f, nil)
}

func TestZSet(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.db.Close()
	key := []byte("testdb_zset_key")
	khash := hash.Fnv32(key)
	member1 := []byte(fmt.Sprintf("a%s", string(testRandBytes(10))))
	member2 := []byte(fmt.Sprintf("b%s", string(testRandBytes(10))))
	member3 := []byte(fmt.Sprintf("c%s", string(testRandBytes(10))))
	member4 := []byte(fmt.Sprintf("d%s", string(testRandBytes(10))))

	n, err := bdb.db.ZAdd(key, khash,
		getFloatByte(0), member1,
		getFloatByte(1), member2,
		getFloatByte(2), member3,
		getFloatByte(3), member4,
	)
	require.NoError(t, err)
	require.Equal(t, int64(4), n)

	n, err = bdb.db.ZRank(key, khash, []byte("xxx"))
	require.Equal(t, btools.ErrZsetMemberNil, err)

	n, err = bdb.db.Exists(key, khash)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, _ = bdb.db.ZCount(key, khash, 0, 0xFF, false, false)
	require.Equal(t, int64(4), n)

	var s float64
	s, err = bdb.db.ZScore(key, khash, member4)
	require.NoError(t, err)
	require.Equal(t, float64(3), s)

	_, err = bdb.db.ZScore(key, khash, []byte("zzz"))
	require.Equal(t, btools.ErrZsetMemberNil, err)

	n, err = bdb.db.ZRem(key, khash, member1, member2)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	n, err = bdb.db.ZRem(key, khash, member1, member2)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	n, err = bdb.db.ZCard(key, khash)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	n, err = bdb.db.Del(khash, key)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, err = bdb.db.Exists(key, khash)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	n, _ = bdb.db.ZCount(key, khash, 0, 0xFF, false, false)
	require.Equal(t, int64(0), n)
}

func TestZSetIncrBy(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()
	key := []byte("testdb_zincrby")
	khash := hash.Fnv32(key)

	total := float64(55)
	for i := 1; i <= 10; i++ {
		tmp := 0
		for j := 1; j <= 10; j++ {
			tmp += j
			if s, err := bdb.db.ZIncrBy(key, khash, float64(j), extend.FormatIntToSlice(i)); err != nil {
				t.Fatal(err)
			} else if s != float64(tmp) {
				t.Fatalf("ZIncrBy err exp:%v act:%v", tmp, s)
			}
		}
	}

	for i := 1; i <= 10; i++ {
		if s, err := bdb.db.ZScore(key, khash, extend.FormatIntToSlice(i)); err != nil {
			t.Fatal(err)
		} else if s != total {
			t.Fatalf("ZScore err exp:%v act:%v", i, s)
		}
	}

	n, _ := bdb.db.ZCount(key, khash, 0, 0xFF, false, false)
	if n != 10 {
		t.Fatal(n)
	}

	if _, err := bdb.db.ZScore(key, khash, []byte("zzz")); err != nil && err != btools.ErrZsetMemberNil {
		t.Fatal(err)
	}

	for i := 1; i <= 6; i += 2 {
		if n, err := bdb.db.ZRem(key, khash, extend.FormatIntToSlice(i), extend.FormatIntToSlice(i+1)); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}
	}

	if n, err := bdb.db.ZCard(key, khash); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := bdb.db.Del(khash, key); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	n, _ = bdb.db.ZCount(key, khash, 0, 0xFF, false, false)
	if n != 0 {
		t.Fatal(n)
	}
}

func TestZSetCmd(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()
	key1 := []byte("testdb_zset_keykind1")
	khash1 := hash.Fnv32(key1)
	key2 := []byte("testdb_zset_keykind2")
	khash2 := hash.Fnv32(key2)
	member1 := []byte(fmt.Sprintf("a%s", string(testRandBytes(10))))
	member2 := []byte(fmt.Sprintf("b%s", string(testRandBytes(10))))
	member3 := []byte(fmt.Sprintf("c%s", string(testRandBytes(10))))
	member4 := []byte(fmt.Sprintf("d%s", string(testRandBytes(10))))

	checkCmd := func(key []byte, khash uint32) {
		n, _ := bdb.db.ZCount(key, khash, 0, 0xFF, false, false)
		require.Equal(t, int64(4), n)

		zpair, zcloser, err := bdb.db.ZRange(key, khash, 0, -1)
		require.NoError(t, err)
		require.Equal(t, 4, len(zpair))
		require.Equal(t, member1, zpair[0].Member)
		require.Equal(t, float64(0), zpair[0].Score)
		require.Equal(t, member2, zpair[1].Member)
		require.Equal(t, float64(1), zpair[1].Score)
		require.Equal(t, member3, zpair[2].Member)
		require.Equal(t, float64(2), zpair[2].Score)
		require.Equal(t, member4, zpair[3].Member)
		require.Equal(t, float64(3), zpair[3].Score)
		zcloser()

		zpair, zcloser, err = bdb.db.ZRevRange(key, khash, 0, -1)
		require.NoError(t, err)
		require.Equal(t, 4, len(zpair))
		require.Equal(t, member4, zpair[0].Member)
		require.Equal(t, float64(3), zpair[0].Score)
		require.Equal(t, member3, zpair[1].Member)
		require.Equal(t, float64(2), zpair[1].Score)
		require.Equal(t, member2, zpair[2].Member)
		require.Equal(t, float64(1), zpair[2].Score)
		require.Equal(t, member1, zpair[3].Member)
		require.Equal(t, float64(0), zpair[3].Score)
		zcloser()

		zpair, zcloser, err = bdb.db.ZRevRange(key, khash, 2, 3)
		require.NoError(t, err)
		require.Equal(t, 2, len(zpair))
		require.Equal(t, member2, zpair[0].Member)
		require.Equal(t, float64(1), zpair[0].Score)
		require.Equal(t, member1, zpair[1].Member)
		require.Equal(t, float64(0), zpair[1].Score)
		zcloser()

		if s, err := bdb.db.ZScore(key, khash, member4); err != nil {
			t.Fatal(err)
		} else if int(s) != 3 {
			t.Fatal(s)
		}

		_, err = bdb.db.ZScore(key, khash, []byte("zzz"))
		require.Equal(t, btools.ErrZsetMemberNil, err)

		if n, err := bdb.db.ZRem(key, khash, member1, member2); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		if n, err := bdb.db.ZRem(key, khash, member1, member2); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if n, err := bdb.db.ZCard(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		if n, err := bdb.db.Del(khash, key); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		n, _ = bdb.db.ZCount(key, khash, 0, 0xFF, false, false)
		if n != 0 {
			t.Fatal(n)
		}
	}

	n, err := bdb.db.ZAdd(key1, khash1,
		getFloatByte(0), member1, getFloatByte(1), member2,
		getFloatByte(2), member3, getFloatByte(3), member4,
	)
	require.NoError(t, err)
	require.Equal(t, int64(4), n)
	checkCmd(key1, khash1)

	n, err = bdb.db.ZAdd(key2, khash2,
		getFloatByte(0), member1, getFloatByte(1), member2,
		getFloatByte(2), member3, getFloatByte(3), member4,
	)
	require.NoError(t, err)
	require.Equal(t, int64(4), n)
	checkCmd(key2, khash2)
}

func TestZSetOrder(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()
	key := []byte("testdb_zset_order")
	khash := hash.Fnv32(key)
	member1 := []byte(fmt.Sprintf("a%s", string(testRandBytes(10))))
	member2 := []byte(fmt.Sprintf("b%s", string(testRandBytes(10))))
	member3 := []byte(fmt.Sprintf("c%s", string(testRandBytes(10))))
	member4 := []byte(fmt.Sprintf("d%s", string(testRandBytes(10))))
	member5 := []byte(fmt.Sprintf("e%s", string(testRandBytes(10))))
	member6 := []byte(fmt.Sprintf("f%s", string(testRandBytes(10))))
	membs := [][]byte{member1, member2, member3, member4, member5, member6}
	membCnt := len(membs)

	for i := 0; i < membCnt; i++ {
		if n, err := bdb.db.ZAdd(key, khash, getFloatByte(float64(i)), membs[i]); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
	}

	if n, _ := bdb.db.ZCount(key, khash, -math.MaxFloat64, math.MaxFloat64, false, false); int(n) != membCnt {
		t.Fatal(n)
	}
	if n, _ := bdb.db.ZCount(key, khash, 0, 5, true, false); n != 5 {
		t.Fatal(n)
	}
	if n, _ := bdb.db.ZCount(key, khash, 0, 5, true, true); n != 4 {
		t.Fatal(n)
	}
	if n, _ := bdb.db.ZCount(key, khash, 0, 5, false, false); n != 6 {
		t.Fatal(n)
	}
	if n, _ := bdb.db.ZCount(key, khash, 0, 5, false, true); n != 5 {
		t.Fatal(n)
	}
	if n, _ := bdb.db.ZCount(key, khash, 0.1, 4, true, true); n != 3 {
		t.Fatal(n)
	}

	for i := 0; i < membCnt; i++ {
		if pos, err := bdb.db.ZRank(key, khash, membs[i]); err != nil {
			t.Fatal(err)
		} else if int(pos) != i {
			t.Fatal(pos)
		}

		pos, err := bdb.db.ZRevRank(key, khash, membs[i])
		if err != nil {
			t.Fatal(err)
		} else if int(pos) != membCnt-i-1 {
			t.Fatal(pos)
		}
	}

	qMembs, closer, err := bdb.db.ZRange(key, khash, 0, -1)
	require.NoError(t, err)
	if len(qMembs) != membCnt {
		t.Fatal(fmt.Sprintf("%d vs %d", len(qMembs), membCnt))
	} else {
		for i := 0; i < membCnt; i++ {
			require.Equal(t, membs[i], qMembs[i].Member)
		}
		closer()
	}

	qMembs, closer, err = bdb.db.ZRevRange(key, khash, 0, -1)
	require.NoError(t, err)
	require.Equal(t, membCnt, len(qMembs))
	for i := 0; i < membCnt; i++ {
		require.Equal(t, membs[membCnt-1-i], qMembs[i].Member)
	}
	closer()

	qMembs, closer, err = bdb.db.ZRangeByScore(key, khash, -1, 0xFFFF, false, false, 0, membCnt)
	require.NoError(t, err)
	require.Equal(t, membCnt, len(qMembs))
	for i := 0; i < membCnt; i++ {
		require.Equal(t, membs[i], qMembs[i].Member)
	}
	closer()

	qMembs, closer, err = bdb.db.ZRevRangeByScore(key, khash, -1, 0xFFFF, false, false, 0, membCnt)
	require.NoError(t, err)
	require.Equal(t, membCnt, len(qMembs))
	for i := 0; i < membCnt; i++ {
		require.Equal(t, membs[membCnt-1-i], qMembs[i].Member)
	}
	closer()

	if n, err := bdb.db.ZAdd(key, khash, getFloatByte(999), member4); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if pos, _ := bdb.db.ZRank(key, khash, member4); pos != int64(membCnt-1) {
		t.Fatal(pos)
	}

	if pos, _ := bdb.db.ZRevRank(key, khash, member4); pos != 0 {
		t.Fatal(pos)
	}

	if pos, _ := bdb.db.ZRank(key, khash, member5); pos != 3 {
		t.Fatal(pos)
	}

	if pos, _ := bdb.db.ZRank(key, khash, member6); pos != 4 {
		t.Fatal(pos)
	}

	qMembs, closer, err = bdb.db.ZRangeByScore(key, khash, 999, 0xFFFF, false, false, 0, membCnt)
	require.NoError(t, err)
	require.Equal(t, 1, len(qMembs))
	closer()

	if s, err := bdb.db.ZIncrBy(key, khash, 2, member5); err != nil {
		t.Fatal(err)
	} else if s != 6 {
		t.Fatal(s)
	}

	if pos, _ := bdb.db.ZRank(key, khash, member5); int(pos) != 4 {
		t.Fatal(pos)
	}

	if pos, _ := bdb.db.ZRevRank(key, khash, member5); int(pos) != 1 {
		t.Fatal(pos)
	}

	res, resCloser, err := bdb.db.ZRange(key, khash, 0, -1)
	require.NoError(t, err)
	require.Equal(t, 6, len(res))
	scores := []int64{0, 1, 2, 5, 6, 999}
	for i := 0; i < len(res); i++ {
		if int64(res[i].Score) != scores[i] {
			t.Fatal(fmt.Sprintf("[%d]=%v", i, res[i]))
		}
	}
	resCloser()
}

func TestZsetScore(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()
	key := []byte("a")
	khash := hash.Fnv32(key)

	member1 := []byte(fmt.Sprintf("a%s", string(testRandBytes(10))))
	member2 := []byte(fmt.Sprintf("b%s", string(testRandBytes(10))))
	member3 := []byte(fmt.Sprintf("c%s", string(testRandBytes(10))))
	member4 := []byte(fmt.Sprintf("d%s", string(testRandBytes(10))))
	member5 := []byte(fmt.Sprintf("e%s", string(testRandBytes(10))))

	if n, err := bdb.db.ZAdd(key, khash, getFloatByte(1), member3); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	if n, err := bdb.db.ZAdd(key, khash, getFloatByte(15), member1); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	if n, err := bdb.db.ZAdd(key, khash, getFloatByte(-15), member5); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	if n, err := bdb.db.ZAdd(key, khash, getFloatByte(0), member4); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	if n, err := bdb.db.ZAdd(key, khash, getFloatByte(13), member2); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	qMembs, closer, err := bdb.db.ZRange(key, khash, 0, -1)
	require.NoError(t, err)
	require.Equal(t, 5, len(qMembs))
	require.Equal(t, member5, qMembs[0].Member)
	require.Equal(t, member4, qMembs[1].Member)
	require.Equal(t, member3, qMembs[2].Member)
	require.Equal(t, member2, qMembs[3].Member)
	require.Equal(t, member1, qMembs[4].Member)
	closer()
}

func TestZSetPersist(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()
	key := []byte("persist")
	khash := hash.Fnv32(key)
	n, err := bdb.db.ZAdd(key, khash, getFloatByte(1), []byte("a"))
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, err = bdb.db.ZCard(key, khash)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	qMembs, closer, err1 := bdb.db.ZRange(key, khash, 0, -1)
	require.NoError(t, err1)
	require.Equal(t, 1, len(qMembs))
	require.Equal(t, []byte("a"), qMembs[0].Member)
	closer()

	n, err = bdb.db.Persist(key, khash)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	_, err = bdb.db.Expire(key, khash, 10)
	require.NoError(t, err)

	n, err = bdb.db.Persist(key, khash)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, err = bdb.db.TTL(key, khash)
	require.NoError(t, err)
	require.Equal(t, int64(-1), n)

	_, err = bdb.db.Expire(key, khash, 2)
	require.NoError(t, err)
	time.Sleep(3 * time.Second)

	n, err = bdb.db.TTL(key, khash)
	require.NoError(t, err)
	require.Equal(t, int64(-2), n)
}

func TestZsetLex(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()
	key := []byte("test_zlex")
	khash := hash.Fnv32(key)
	score := numeric.Float64ToByteSort(0, nil)
	if n, err := bdb.db.ZAdd(key, khash,
		score, []byte("a"), score, []byte("b"), score, []byte("c"),
		score, []byte("d"), score, []byte("e"), score, []byte("f"), score, []byte("g"),
	); err != nil {
		t.Fatal(err)
	} else if n != 7 {
		t.Fatal(n)
	}

	res, closer, err := bdb.db.ZRangeByLex(key, khash, nil, []byte("c"), false, false, 0, 100)
	require.NoError(t, err)
	require.Equal(t, 3, len(res))
	require.Equal(t, []byte("a"), res[0])
	require.Equal(t, []byte("b"), res[1])
	require.Equal(t, []byte("c"), res[2])
	closer()

	res, closer, err = bdb.db.ZRangeByLex(key, khash, nil, []byte("c"), false, true, 0, 100)
	require.NoError(t, err)
	require.Equal(t, 2, len(res))
	require.Equal(t, []byte("a"), res[0])
	require.Equal(t, []byte("b"), res[1])
	closer()

	res, closer, _ = bdb.db.ZRangeByLex(key, khash, []byte("aaa"), []byte("g"), false, true, 0, 100)
	require.NoError(t, err)
	require.Equal(t, 5, len(res))
	require.Equal(t, []byte("b"), res[0])
	require.Equal(t, []byte("c"), res[1])
	require.Equal(t, []byte("d"), res[2])
	require.Equal(t, []byte("e"), res[3])
	require.Equal(t, []byte("f"), res[4])
	closer()

	n, err := bdb.db.ZLexCount(key, khash, []byte{'-'}, []byte{'+'}, false, false)
	require.NoError(t, err)
	require.Equal(t, int64(7), n)

	n, err = bdb.db.ZRemRangeByLex(key, khash, []byte("aaa"), []byte("g"), false, true)
	require.NoError(t, err)
	require.Equal(t, int64(5), n)

	n, _ = bdb.db.ZLexCount(key, khash, []byte{'-'}, []byte{'+'}, false, false)
	require.Equal(t, int64(2), n)
}

func TestZsetExists(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()
	key := []byte("test_zset_exists")
	khash := hash.Fnv32(key)
	if n, err := bdb.db.Exists(key, khash); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal("invalid value ", n)
	}

	score := numeric.Float64ToByteSort(0, nil)
	bdb.db.ZAdd(key, khash, score, []byte("a"), score, []byte("b"))
	if n, err := bdb.db.Exists(key, khash); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal("invalid value ", n)
	}
}

//func TestZsetZScan(t *testing.T) {
//	bdb := testNewNoCacheBitsDB()
//	defer bdb.Close()
//	kn := 1
//	kkn := 100
//	itemList := testMakeItemList(kn, kkn, btools.DataTypeZset, 20, 0)
//	for _, item := range itemList {
//		zn, err := bdb.db.ZAdd(item.key, item.keyHash, item.kvs...)
//		require.NoError(t, err)
//		require.Equal(t, int64(kkn), zn)
//	}
//
//	step := 15
//	start := 0
//	var cursor []byte
//	var res []btools.ScorePair
//	var err error
//	for _, item := range itemList {
//		cursor = nil
//		for {
//			cursor, res, err = bdb.db.ZScan(item.key, item.keyHash, cursor, step, "*")
//			require.NoError(t, err)
//			start += step
//			if start > 100 {
//				require.Equal(t, 10, len(res))
//				require.Equal(t, btools.ScanEndCurosr, cursor)
//				break
//			} else {
//				require.Equal(t, 15, len(res))
//			}
//		}
//	}
//}

//func TestZsetScan(t *testing.T) {
//	bdb := testNewNoCacheBitsDB()
//	defer bdb.Close()
//	_, oldV, _ := bdb.db.Scan(nil, 100, "", btools.DtZset)
//	for _, d := range oldV {
//		dhash := hash.Fnv32(d)
//		bdb.db.Del(dhash, d)
//	}
//
//	key := []byte("scan_aaa")
//	key1 := []byte("scan_bbb")
//	khash := hash.Fnv32(key)
//	k1hash := hash.Fnv32(key1)
//
//	bdb.db.ZAdd(key, khash,
//		getFloatByte(1), []byte("1"),
//		getFloatByte(2), []byte("222"),
//		getFloatByte(3), []byte("19"),
//		getFloatByte(4), []byte("1234"),
//	)
//	bdb.db.ZAdd(key1, k1hash,
//		getFloatByte(10), []byte("fff"),
//		getFloatByte(20), []byte("ggg"),
//	)
//
//	cursor, v, err := bdb.db.Scan(nil, 100, "", btools.DtZset)
//	if err != nil {
//		t.Fatal(err)
//	} else if len(v) != 2 {
//		t.Fatal("invalid count", len(v))
//	}
//
//	cursor, v, err = bdb.db.Scan([]byte("scan_aaa"), 1, "**", btools.DtZset)
//	if err != nil {
//		t.Fatal(err)
//	} else if len(v) != 1 {
//		t.Fatal("invalid count", len(v))
//	} else if string(v[0]) != "scan_aaa" {
//		t.Fatal(string(v[0]))
//	} else if string(cursor) != "scan_bbb" {
//		t.Fatal(cursor)
//	}
//}

func TestZsetZRem(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()
	key := []byte("test_zset_zrem")
	khash := hash.Fnv32(key)
	member1 := []byte(fmt.Sprintf("a%s", string(testRandBytes(10))))
	member2 := []byte(fmt.Sprintf("b%s", string(testRandBytes(10))))
	member3 := []byte(fmt.Sprintf("c%s", string(testRandBytes(10))))
	member4 := []byte(fmt.Sprintf("d%s", string(testRandBytes(10))))

	n, err := bdb.db.ZAdd(key, khash,
		getFloatByte(0), member1,
		getFloatByte(1), member2,
		getFloatByte(2), member3,
		getFloatByte(3), member4,
	)
	require.NoError(t, err)
	require.Equal(t, int64(4), n)

	n, err = bdb.db.ZRemRangeByRank(key, khash, 0, 1)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	res, resCloser, err1 := bdb.db.ZRange(key, khash, 0, 0xff)
	require.NoError(t, err1)
	require.Equal(t, 2, len(res))
	require.Equal(t, member3, res[0].Member)
	require.Equal(t, member4, res[1].Member)
	resCloser()

	n, err = bdb.db.ZRemRangeByScore(key, khash, 0, 2, false, true)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	n, err = bdb.db.ZRemRangeByScore(key, khash, 0, 2, false, false)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	res, resCloser, err = bdb.db.ZRange(key, khash, 0, 0xff)
	require.NoError(t, err)
	require.Equal(t, 1, len(res))
	require.Equal(t, member4, res[0].Member)
	resCloser()

	n, err = bdb.db.ZCard(key, khash)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, err = bdb.db.Del(khash, key)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, err = bdb.db.ZCount(key, khash, 0, 0xFF, false, false)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
}

func testRandValue(len int) []byte {
	val := make([]byte, len)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		val[i] = byte(b)
	}
	return val
}

func TestZsetZAddMulti(t *testing.T) {
	var kvZAddBase atomic.Int64
	var kvZAddFieldBase atomic.Int64
	fieldNum := int64(20)
	fieldSize := 100
	fieldPrefix := testRandValue(fieldSize)
	kvZAddBase.Store(-1)

	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	makeKey := func(keyIndex int64) []byte {
		key := "zset" + strconv.FormatInt(keyIndex, 10)
		v := md5.Sum([]byte(key))
		return []byte(fmt.Sprintf("%x", v[0:16]))
	}

	f := func() {
		n := kvZAddBase.Add(1)
		score := kvZAddFieldBase.Add(1)
		keyIndex := n / fieldNum
		wkey := makeKey(keyIndex)
		khash := hash.Fnv32(wkey)
		member := make([]byte, fieldSize)
		copy(member, fieldPrefix[0:fieldSize-8])
		binary.BigEndian.PutUint64(member[fieldSize-8:], uint64(score))
		res, err := bdb.db.ZAdd(wkey, khash, numeric.Float64ToByteSort(float64(score), nil), member)
		require.NoError(t, err)
		require.Equal(t, int64(1), res)
	}

	var wg sync.WaitGroup
	cnum := 50
	loop := 1000 / cnum
	wg.Add(cnum)
	for i := 0; i < cnum; i++ {
		go func() {
			for j := 0; j < loop; j++ {
				f()
			}
			wg.Done()
		}()
	}
	wg.Wait()

	keyNum := kvZAddBase.Load()
	for i := int64(0); i < keyNum; i += fieldNum {
		keyIndex := i / fieldNum
		wkey := makeKey(keyIndex)
		khash := hash.Fnv32(wkey)
		size, err := bdb.db.ZCard(wkey, khash)
		require.NoError(t, err)
		require.Equal(t, fieldNum, size)
	}
}
