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
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
)

func spair(score float64, member []byte) btools.ScorePair {
	return btools.ScorePair{Score: score, Member: member}
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

func TestZSetCodec(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("key")
		member := []byte("member")
		keyId := bdb.ZsetObj.GetNextKeyId()
		mkv := &base.MetaData{}
		mkv.SetDataType(btools.ZSET)
		mkv.Reset(keyId)
		keyVersion := mkv.Version()
		keyKind := mkv.Kind()
		khash := hash.Fnv32(key)
		ek, ekCloser := base.EncodeMetaKey(key, khash)
		defer ekCloser()
		if k, err := base.DecodeMetaKey(ek); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(k, key) {
			t.Fatal("key is not eq", key, k)
		}

		var verBytes [8]byte
		binary.LittleEndian.PutUint64(verBytes[:], keyVersion)
		verMember := append(member, verBytes[:]...)
		memberMd5 := md5.Sum(verMember)

		var edk [base.DataKeyZsetLength]byte
		base.EncodeZsetDataKey(edk[:], keyVersion, khash, member)
		if m, err := base.DecodeZsetDataKey(edk[:]); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(m, memberMd5[:]) {
			t.Fatal("memberMd5 err", m, memberMd5[:])
		}

		efk, efkCloser, isCompress := base.EncodeZsetIndexKey(keyVersion, keyKind, khash, 100, member)
		require.Equal(t, false, isCompress)
		ver, score, fp := base.DecodeZsetIndexKey(keyKind, efk, []byte{base.DataValueKindDefault})
		if ver != keyVersion {
			t.Fatal("version err")
		} else if !fp.Equal(member) {
			t.Fatal("member err")
		} else if score != 100 {
			t.Fatal(score)
		}
		efkCloser()

		member = testRandBytes(base.KeyFieldCompressSize * 2)
		keyVersion = base.EncodeKeyVersion(bdb.ZsetObj.GetNextKeyId(), keyKind)
		efk, efkCloser, isCompress = base.EncodeZsetIndexKey(keyVersion, keyKind, khash, 100, member)
		if keyKind == base.KeyKindDefault {
			require.Equal(t, false, isCompress)
		} else {
			require.Equal(t, true, isCompress)
		}
		value := make([]byte, len(member)-base.KeyFieldCompressPrefix+1)
		value[0] = base.DataValueKindFieldCompress
		copy(value[1:], member[base.KeyFieldCompressPrefix:])
		ver, score, fp = base.DecodeZsetIndexKey(keyKind, efk, value)
		if ver != keyVersion {
			t.Fatal("version err")
		} else if !fp.Equal(member) {
			t.Fatal("member err")
		} else if score != 100 {
			t.Fatal(score)
		}
		efkCloser()
	}
}

func TestDBZSet(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("testdb_zset_a")
		khash := hash.Fnv32(key)
		member1 := []byte(fmt.Sprintf("a%s", string(testRandBytes(10))))
		member2 := []byte(fmt.Sprintf("b%s", string(testRandBytes(base.KeyFieldCompressSize-1))))
		member3 := []byte(fmt.Sprintf("c%s", string(testRandBytes(base.KeyFieldCompressSize))))
		member4 := []byte(fmt.Sprintf("d%s", string(testRandBytes(base.KeyFieldCompressSize*2))))

		if n, err := bdb.ZsetObj.ZAdd(key, khash,
			spair(0, member1),
			spair(1, member2),
			spair(2, member3),
			spair(3, member4),
		); err != nil {
			t.Fatal(err)
		} else if n != 4 {
			t.Fatal(n)
		}

		mk, mkCloser := base.EncodeMetaKey(key, khash)
		mkv, err := bdb.ZsetObj.GetMetaData(mk)
		mkCloser()
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, base.KeyKindFieldCompress, mkv.Kind())

		if n, err := bdb.ZsetObj.ZCount(key, khash, 0, 0xFF, false, false); err != nil {
			t.Fatal(err)
		} else if n != 4 {
			t.Fatal(n)
		}

		if s, err := bdb.ZsetObj.ZScore(key, khash, member4); err != nil {
			t.Fatal(err)
		} else if int(s) != 3 {
			t.Fatal(s)
		}

		if s, err := bdb.ZsetObj.ZScore(key, khash, []byte("zzz")); err != nil && err != errn.ErrZsetMemberNil {
			t.Fatal(fmt.Sprintf("s=[%d] err=[%s]", int(s), err))
		}

		if n, err := bdb.ZsetObj.ZRem(key, khash, member1, member2); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		if n, err := bdb.ZsetObj.ZRem(key, khash, member1, member2); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if n, err := bdb.ZsetObj.ZCard(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		if n, err := bdb.StringObj.Del(khash, key); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := bdb.ZsetObj.ZCount(key, khash, 0, 0xFF, false, false); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}
}

func TestDBZSetKeyKind(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key1 := []byte("testdb_zset_keykind1")
		khash1 := hash.Fnv32(key1)
		key2 := []byte("testdb_zset_keykind2")
		khash2 := hash.Fnv32(key2)
		member1 := []byte(fmt.Sprintf("a%s", string(testRandBytes(10))))
		member2 := []byte(fmt.Sprintf("b%s", string(testRandBytes(base.KeyFieldCompressSize-1))))
		member3 := []byte(fmt.Sprintf("c%s", string(testRandBytes(base.KeyFieldCompressSize))))
		member4 := []byte(fmt.Sprintf("d%s", string(testRandBytes(base.KeyFieldCompressSize*2))))

		if n, err := bdb.ZsetObj.ZAdd(key1, khash1,
			spair(0, member1),
			spair(1, member2),
			spair(2, member3),
			spair(3, member4),
		); err != nil {
			t.Fatal(err)
		} else if n != 4 {
			t.Fatal(n)
		}

		checkKeyKind := func(k []byte, h uint32, kind uint8) {
			mk, mkCloser := base.EncodeMetaKey(k, h)
			mkv, err := bdb.ZsetObj.GetMetaData(mk)
			mkCloser()
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, kind, mkv.Kind())
		}

		checkCmd := func(key []byte, khash uint32, kind uint8) {
			checkKeyKind(key, khash, kind)
			if n, err := bdb.ZsetObj.ZCount(key, khash, 0, 0xFF, false, false); err != nil {
				t.Fatal(err)
			} else if n != 4 {
				t.Fatal(n)
			}

			if zpair, err := bdb.ZsetObj.ZRange(key, khash, 0, -1); err != nil {
				t.Fatal(err)
			} else if len(zpair) != 4 {
				t.Fatal("zrange len err", len(zpair))
			} else if !bytes.Equal(member1, zpair[0].Member) {
				t.Fatal("zrange 0 member err", string(zpair[0].Member))
			} else if 0 != zpair[0].Score {
				t.Fatal("zrange 0 score err", zpair[0].Score)
			} else if !bytes.Equal(member2, zpair[1].Member) {
				t.Fatal("zrange 1 member err", string(zpair[1].Member))
			} else if 1 != zpair[1].Score {
				t.Fatal("zrange 1 score err", zpair[1].Score)
			} else if !bytes.Equal(member3, zpair[2].Member) {
				t.Fatal("zrange 2 member err", string(zpair[2].Member))
			} else if 2 != zpair[2].Score {
				t.Fatal("zrange 2 score err", zpair[2].Score)
			} else if !bytes.Equal(member4, zpair[3].Member) {
				t.Fatal("zrange 3 member err", string(zpair[3].Member))
			} else if 3 != zpair[3].Score {
				t.Fatal("zrange 3 score err", zpair[3].Score)
			}

			if zpair, err := bdb.ZsetObj.ZRevRange(key, khash, 0, -1); err != nil {
				t.Fatal(err)
			} else if len(zpair) != 4 {
				t.Fatal("zrevrange len err", len(zpair))
			} else if !bytes.Equal(member4, zpair[0].Member) {
				t.Fatal("zrevrange 0 member err", string(zpair[0].Member))
			} else if 3 != zpair[0].Score {
				t.Fatal("zrevrange 0 score err", zpair[0].Score)
			} else if !bytes.Equal(member3, zpair[1].Member) {
				t.Fatal("zrevrange 1 member err", string(zpair[1].Member))
			} else if 2 != zpair[1].Score {
				t.Fatal("zrevrange 1 score err", zpair[1].Score)
			} else if !bytes.Equal(member2, zpair[2].Member) {
				t.Fatal("zrevrange 2 member err", string(zpair[2].Member))
			} else if 1 != zpair[2].Score {
				t.Fatal("zrevrange 2 score err", zpair[2].Score)
			} else if !bytes.Equal(member1, zpair[3].Member) {
				t.Fatal("zrevrange 3 member err", string(zpair[3].Member))
			} else if 0 != zpair[3].Score {
				t.Fatal("zrevrange 3 score err", zpair[3].Score)
			}

			if s, err := bdb.ZsetObj.ZScore(key, khash, member4); err != nil {
				t.Fatal(err)
			} else if int(s) != 3 {
				t.Fatal(s)
			}

			if s, err := bdb.ZsetObj.ZScore(key, khash, []byte("zzz")); err != nil && err != errn.ErrZsetMemberNil {
				t.Fatal(fmt.Sprintf("s=[%d] err=[%s]", int(s), err))
			}

			if n, err := bdb.ZsetObj.ZRem(key, khash, member1, member2); err != nil {
				t.Fatal(err)
			} else if n != 2 {
				t.Fatal(n)
			}

			if n, err := bdb.ZsetObj.ZRem(key, khash, member1, member2); err != nil {
				t.Fatal(err)
			} else if n != 0 {
				t.Fatal(n)
			}

			if n, err := bdb.ZsetObj.ZCard(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 2 {
				t.Fatal(n)
			}

			checkKeyKind(key, khash, kind)

			if n, err := bdb.StringObj.Del(khash, key); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}

			if n, err := bdb.ZsetObj.ZCount(key, khash, 0, 0xFF, false, false); err != nil {
				t.Fatal(err)
			} else if n != 0 {
				t.Fatal(n)
			}
		}

		checkCmd(key1, khash1, base.KeyKindFieldCompress)

		if n, err := bdb.ZsetObj.ZAdd(key2, khash2,
			spair(0, member1),
			spair(1, member2),
			spair(2, member3),
			spair(3, member4),
		); err != nil {
			t.Fatal(err)
		} else if n != 4 {
			t.Fatal(n)
		}
		checkCmd(key2, khash2, base.KeyKindFieldCompress)
	}
}

func TestZSetOrder(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("testdb_zset_order")
		khash := hash.Fnv32(key)
		member1 := []byte(fmt.Sprintf("a%s", string(testRandBytes(10))))
		member2 := []byte(fmt.Sprintf("b%s", string(testRandBytes(base.KeyFieldCompressSize/2))))
		member3 := []byte(fmt.Sprintf("c%s", string(testRandBytes(base.KeyFieldCompressSize-1))))
		member4 := []byte(fmt.Sprintf("d%s", string(testRandBytes(base.KeyFieldCompressSize))))
		member5 := []byte(fmt.Sprintf("e%s", string(testRandBytes(base.KeyFieldCompressSize*2))))
		member6 := []byte(fmt.Sprintf("f%s", string(testRandBytes(base.KeyFieldCompressSize*5))))
		membs := [][]byte{member1, member2, member3, member4, member5, member6}
		membCnt := len(membs)

		for i := 0; i < membCnt; i++ {
			if n, err := bdb.ZsetObj.ZAdd(key, khash, spair(float64(i), membs[i])); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}
		}

		if n, _ := bdb.ZsetObj.ZCount(key, khash, -math.MaxFloat64, math.MaxFloat64, false, false); int(n) != membCnt {
			t.Fatal(n)
		}
		if n, _ := bdb.ZsetObj.ZCount(key, khash, 0, 5, true, false); n != 5 {
			t.Fatal(n)
		}
		if n, _ := bdb.ZsetObj.ZCount(key, khash, 0, 5, true, true); n != 4 {
			t.Fatal(n)
		}
		if n, _ := bdb.ZsetObj.ZCount(key, khash, 0, 5, false, false); n != 6 {
			t.Fatal(n)
		}
		if n, _ := bdb.ZsetObj.ZCount(key, khash, 0, 5, false, true); n != 5 {
			t.Fatal(n)
		}
		if n, _ := bdb.ZsetObj.ZCount(key, khash, 0.1, 4, true, true); n != 3 {
			t.Fatal(n)
		}

		for i := 0; i < membCnt; i++ {
			if pos, err := bdb.ZsetObj.ZRank(key, khash, membs[i]); err != nil {
				t.Fatal(err)
			} else if int(pos) != i {
				t.Fatal(pos)
			}

			pos, err := bdb.ZsetObj.ZRevRank(key, khash, membs[i])
			if err != nil {
				t.Fatal(err)
			} else if int(pos) != membCnt-i-1 {
				t.Fatal(pos)
			}
		}

		if qMembs, err := bdb.ZsetObj.ZRange(key, khash, 0, -1); err != nil {
			t.Fatal(err)
		} else if len(qMembs) != membCnt {
			t.Fatal(fmt.Sprintf("%d vs %d", len(qMembs), membCnt))
		} else {
			for i := 0; i < membCnt; i++ {
				if !bytes.Equal(membs[i], qMembs[i].Member) {
					t.Fatal("ZRange member not eq", i)
				}
			}
		}

		if qMembs, err := bdb.ZsetObj.ZRevRange(key, khash, 0, -1); err != nil {
			t.Fatal(err)
		} else if len(qMembs) != membCnt {
			t.Fatal(fmt.Sprintf("%d vs %d", len(qMembs), membCnt))
		} else {
			for i := 0; i < membCnt; i++ {
				if !bytes.Equal(membs[membCnt-1-i], qMembs[i].Member) {
					t.Fatal("ZRevRange member not eq", i)
				}
			}
		}

		if qMembs, err := bdb.ZsetObj.ZRangeByScore(key, khash, -1, 0xFFFF, false, false, 0, membCnt); err != nil {
			t.Fatal(err)
		} else if len(qMembs) != membCnt {
			t.Fatal(fmt.Sprintf("%d vs %d", len(qMembs), membCnt))
		} else {
			for i := 0; i < membCnt; i++ {
				if !bytes.Equal(membs[i], qMembs[i].Member) {
					t.Fatal("ZRangeByScore member not eq", i)
				}
			}
		}

		if qMembs, err := bdb.ZsetObj.ZRevRangeByScore(key, khash, -1, 0xFFFF, false, false, 0, membCnt); err != nil {
			t.Fatal(err)
		} else if len(qMembs) != membCnt {
			t.Fatal(fmt.Sprintf("%d vs %d", len(qMembs), membCnt))
		} else {
			for i := 0; i < membCnt; i++ {
				if !bytes.Equal(membs[membCnt-1-i], qMembs[i].Member) {
					t.Fatal("ZRevRangeByScore member not eq", i)
				}
			}
		}

		if n, err := bdb.ZsetObj.ZAdd(key, khash, spair(999, member4)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if pos, _ := bdb.ZsetObj.ZRank(key, khash, member4); pos != int64(membCnt-1) {
			t.Fatal(pos)
		}

		if pos, _ := bdb.ZsetObj.ZRevRank(key, khash, member4); pos != 0 {
			t.Fatal(pos)
		}

		if pos, _ := bdb.ZsetObj.ZRank(key, khash, member5); pos != 3 {
			t.Fatal(pos)
		}

		if pos, _ := bdb.ZsetObj.ZRank(key, khash, member6); pos != 4 {
			t.Fatal(pos)
		}

		if qMembs, err := bdb.ZsetObj.ZRangeByScore(key, khash, 999, 0xFFFF, false, false, 0, membCnt); err != nil {
			t.Fatal(err)
		} else if len(qMembs) != 1 {
			t.Fatal(len(qMembs))
		}

		if s, err := bdb.ZsetObj.ZIncrBy(key, khash, 2, member5); err != nil {
			t.Fatal(err)
		} else if s != 6 {
			t.Fatal(s)
		}

		if pos, _ := bdb.ZsetObj.ZRank(key, khash, member5); int(pos) != 4 {
			t.Fatal(pos)
		}

		if pos, _ := bdb.ZsetObj.ZRevRank(key, khash, member5); int(pos) != 1 {
			t.Fatal(pos)
		}

		if datas, _ := bdb.ZsetObj.ZRange(key, khash, 0, -1); len(datas) != 6 {
			t.Fatal(len(datas))
		} else {
			scores := []int64{0, 1, 2, 5, 6, 999}
			for i := 0; i < len(datas); i++ {
				if int64(datas[i].Score) != scores[i] {
					t.Fatal(fmt.Sprintf("[%d]=%v", i, datas[i]))
				}
			}
		}
	}
}

func TestZsetIncrby(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db
		incrKey := []byte("zset_incrby_case")
		khash := hash.Fnv32(incrKey)
		member := []byte("zincr_field")

		newScore, err := bdb.ZsetObj.ZIncrBy(incrKey, khash, 1, member)
		if err != nil {
			t.Fatal(err)
		} else if newScore != 1 {
			t.Fatal("newScore != 1")
		}

		member2 := []byte("zincr_field_two")
		_, err = bdb.ZsetObj.ZIncrBy(incrKey, khash, 1, member2)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestZsetScore(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("a")
		khash := hash.Fnv32(key)

		member1 := []byte(fmt.Sprintf("a%s", string(testRandBytes(base.KeyFieldCompressSize-10))))
		member2 := []byte(fmt.Sprintf("b%s", string(testRandBytes(10))))
		member3 := []byte(fmt.Sprintf("c%s", string(testRandBytes(base.KeyFieldCompressSize*10))))
		member4 := []byte(fmt.Sprintf("d%s", string(testRandBytes(base.KeyFieldCompressSize-1))))
		member5 := []byte(fmt.Sprintf("e%s", string(testRandBytes(base.KeyFieldCompressSize*2))))
		if n, err := bdb.ZsetObj.ZAdd(key, khash, spair(1, member3)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.ZsetObj.ZAdd(key, khash, spair(15, member1)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.ZsetObj.ZAdd(key, khash, spair(-15, member5)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.ZsetObj.ZAdd(key, khash, spair(0, member4)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.ZsetObj.ZAdd(key, khash, spair(13, member2)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if qMembs, err := bdb.ZsetObj.ZRange(key, khash, 0, -1); err != nil {
			t.Fatal(err)
		} else if len(qMembs) != 5 {
			t.Fatal(fmt.Sprintf("%d vs %d", len(qMembs), 1))
		} else {
			if !bytes.Equal(qMembs[0].Member, member5) {
				t.Fatal("ZRange 0 member err", string(qMembs[0].Member))
			}
			if !bytes.Equal(qMembs[1].Member, member4) {
				t.Fatal("ZRange 1 member err", string(qMembs[1].Member))
			}
			if !bytes.Equal(qMembs[2].Member, member3) {
				t.Fatal("ZRange 2 member err", string(qMembs[2].Member))
			}
			if !bytes.Equal(qMembs[3].Member, member2) {
				t.Fatal("ZRange 3 member err", string(qMembs[3].Member))
			}
			if !bytes.Equal(qMembs[4].Member, member1) {
				t.Fatal("ZRange 4 member err", string(qMembs[4].Member))
			}
		}
	}
}

func TestZSetPersist(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("persist")
		khash := hash.Fnv32(key)
		if n, err := bdb.ZsetObj.ZAdd(key, khash, spair(1, []byte("a"))); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := bdb.ZsetObj.ZCard(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if qMembs, err := bdb.ZsetObj.ZRange(key, khash, 0, -1); err != nil {
			t.Fatal(err)
		} else if len(qMembs) != 1 {
			t.Fatal(fmt.Sprintf("%d vs %d", len(qMembs), 1))
		} else {
			for i := 0; i < 1; i++ {
				if string(qMembs[i].Member) != "a" {
					t.Fatalf("[%v] vs [%v]", qMembs[i], "a")
				}
			}
		}

		if n, err := bdb.StringObj.BasePersist(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if _, err := bdb.StringObj.Expire(key, khash, 10); err != nil {
			t.Fatal(err)
		}

		if n, err := bdb.StringObj.BasePersist(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := bdb.StringObj.TTL(key, khash); err != nil {
			t.Fatal(err)
		} else if n != -1 {
			t.Fatal(n)
		}
	}
}

func TestZLex(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("myzset")
		khash := hash.Fnv32(key)
		if _, err := bdb.ZsetObj.ZAdd(key, khash,
			btools.ScorePair{0, []byte("a")},
			btools.ScorePair{0, []byte("b")},
			btools.ScorePair{0, []byte("c")},
			btools.ScorePair{0, []byte("d")},
			btools.ScorePair{0, []byte("e")},
			btools.ScorePair{0, []byte("f")},
			btools.ScorePair{0, []byte("g")}); err != nil {
			t.Fatal(err)
		}

		if ay, err := bdb.ZsetObj.ZRangeByLex(key, khash, nil, []byte("c"), false, false, 0, 100); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(ay, [][]byte{[]byte("a"), []byte("b"), []byte("c")}) {
			t.Fatal("must equal a, b, c", ay)
		}

		if ay, err := bdb.ZsetObj.ZRangeByLex(key, khash, nil, []byte("c"), false, true, 0, 100); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(ay, [][]byte{[]byte("a"), []byte("b")}) {
			t.Fatal("must equal a, b")
		}

		if ay, err := bdb.ZsetObj.ZRangeByLex(key, khash, []byte("aaa"), []byte("g"), false, true, 0, 100); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(ay, [][]byte{[]byte("b"),
			[]byte("c"), []byte("d"), []byte("e"), []byte("f")}) {
			t.Fatal("must equal b, c, d, e, f", fmt.Sprintf("%q", ay))
		}

		if n, err := bdb.ZsetObj.ZLexCount(key, khash, []byte{'-'}, []byte{'+'}, false, false); err != nil {
			t.Fatal(err)
		} else if n != 7 {
			t.Fatal(n)
		}

		if n, err := bdb.ZsetObj.ZRemRangeByLex(key, khash, []byte("aaa"), []byte("g"), false, true); err != nil {
			t.Fatal(err)
		} else if n != 5 {
			t.Fatal(n)
		}

		if n, err := bdb.ZsetObj.ZLexCount(key, khash, []byte{'-'}, []byte{'+'}, false, false); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}
	}
}

func TestZsetExists(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("zkeyexists_test")
		khash := hash.Fnv32(key)
		if n, err := bdb.StringObj.Exists(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal("invalid value ", n)
		}

		bdb.ZsetObj.ZAdd(key, khash, spair(0, []byte("a")), spair(0, []byte("b")))
		if n, err := bdb.StringObj.Exists(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal("invalid value ", n)
		}
	}
}

func TestZsetDBZScan(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("zscan_z_key")
		key1 := []byte("myzset")
		khash := hash.Fnv32(key)
		k1hash := hash.Fnv32(key1)
		defer bdb.StringObj.Del(khash, key, key1)
		bdb.ZsetObj.ZAdd(key, khash,
			spair(1, []byte("1")),
			spair(2, []byte("222")),
			spair(3, []byte("19")),
			spair(4, []byte("1234")))
		keyKind := base.KeyKindFieldCompress
		keyVersion := base.EncodeKeyVersion(bdb.ZsetObj.GetCurrentKeyId(), keyKind)
		bdb.ZsetObj.ZAdd(key1, k1hash, spair(10, []byte("fff")), spair(20, []byte("ggg")))
		cursor, v, err := bdb.ZsetObj.ZScan(key, khash, nil, 100, "*")
		if err != nil {
			t.Fatal(err)
		} else if len(v) != 4 {
			t.Fatal("invalid count", len(v))
		}

		seek, seekCloser, _ := base.EncodeZsetIndexKey(keyVersion, keyKind, khash, 3, []byte("19"))
		defer seekCloser()
		_, _, _, seekCursor := base.DecodeZsetIndexKeyByCursor(keyKind, seek, base.NilDataVal)
		seekNext, seekNextCloser, _ := base.EncodeZsetIndexKey(keyVersion, keyKind, khash, 4, []byte("1234"))
		defer seekNextCloser()
		_, _, _, seekNextCursor := base.DecodeZsetIndexKeyByCursor(keyKind, seekNext, base.NilDataVal)
		cursor, v, err = bdb.ZsetObj.ZScan(key, khash, seekCursor, 1, "*")
		if err != nil {
			t.Fatal(err)
		} else if len(v) != 1 {
			t.Fatal("invalid count", len(v))
		} else if v[0].Score != 3 {
			t.Fatal("score err", v[0].Score)
		} else if !bytes.Equal(v[0].Member, []byte("19")) {
			t.Fatal("member err", string(v[0].Member))
		} else if !bytes.Equal(cursor, seekNextCursor) {
			t.Fatal("cursor err", seekNextCursor, cursor)
		}
	}
}

func TestZsetDBScan(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		_, oldV, _ := bdb.Scan(nil, 100, "", btools.ZSET)
		for _, d := range oldV {
			dhash := hash.Fnv32(d)
			bdb.StringObj.Del(dhash, d)
		}

		key := []byte("scan_aaa")
		key1 := []byte("scan_bbb")
		khash := hash.Fnv32(key)
		k1hash := hash.Fnv32(key1)
		bdb.ZsetObj.ZAdd(key, khash,
			spair(1, []byte("1")),
			spair(2, []byte("222")),
			spair(3, []byte("19")),
			spair(4, []byte("1234")))
		bdb.ZsetObj.ZAdd(key1, k1hash, spair(10, []byte("fff")), spair(20, []byte("ggg")))
		cursor, v, err := bdb.Scan(nil, 100, "", btools.ZSET)
		if err != nil {
			t.Fatal(err)
		} else if len(v) != 2 {
			t.Fatal("invalid count", len(v))
		}

		cursor, v, err = bdb.Scan([]byte("scan_aaa"), 1, "**", btools.ZSET)
		t.Log(string(cursor))
		for _, d := range v {
			t.Log(string(d))
		}
		if err != nil {
			t.Fatal(err)
		} else if len(v) != 1 {
			t.Fatal("invalid count", len(v))
		} else if string(v[0]) != "scan_aaa" {
			t.Fatal(string(v[0]))
		} else if string(cursor) != "scan_bbb" {
			t.Fatal(cursor)
		}
	}
}

func TestZsetZRem(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("testdb_zrem_a")
		khash := hash.Fnv32(key)
		member1 := []byte(fmt.Sprintf("a%s", string(testRandBytes(10))))
		member2 := []byte(fmt.Sprintf("b%s", string(testRandBytes(base.KeyFieldCompressSize-1))))
		member3 := []byte(fmt.Sprintf("c%s", string(testRandBytes(base.KeyFieldCompressSize))))
		member4 := []byte(fmt.Sprintf("d%s", string(testRandBytes(base.KeyFieldCompressSize*2))))

		if n, err := bdb.ZsetObj.ZAdd(key, khash,
			spair(0, member1),
			spair(1, member2),
			spair(2, member3),
			spair(3, member4),
		); err != nil {
			t.Fatal(err)
		} else if n != 4 {
			t.Fatal(n)
		}

		if n, err := bdb.ZsetObj.ZRemRangeByRank(key, khash, 0, 1); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		if res, err := bdb.ZsetObj.ZRange(key, khash, 0, 0xff); err == nil {
			if !bytes.Equal(res[0].Member, member3) && !bytes.Equal(res[1].Member, member4) {
				t.Fatal("member error")
			}
		}

		if n, err := bdb.ZsetObj.ZRemRangeByScore(key, khash, 0, 2, false, false); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatalf("actual(%d) vs expect(%d)", n, 1)
		}

		if res, err := bdb.ZsetObj.ZRange(key, khash, 0, 0xff); err == nil {
			require.Equal(t, member4, res[0].Member)
		}

		if n, err := bdb.ZsetObj.ZCard(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := bdb.StringObj.Del(khash, key); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := bdb.ZsetObj.ZCount(key, khash, 0, 0xFF, false, false); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}
}
