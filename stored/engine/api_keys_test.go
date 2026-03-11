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
	"testing"
	"time"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb"
	"github.com/zuoyebang/bitalostored/stored/engine/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"

	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/stretchr/testify/require"
)

func TestKeys_Expire_Persist_TTL_Type(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		checkKey := func(key []byte, khash uint32, dt string) {
			if n, err := bdb.Persist(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 0 {
				t.Fatal(n)
			}

			if tp, err := bdb.Type(key, khash); err != nil {
				t.Fatal(err)
			} else if tp != dt {
				t.Fatalf("type fail exp:%s act:%s", dt, tp)
			}

			if n, err := bdb.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != bitsdb.ErrnoKeyPersist {
				t.Fatal(n)
			}

			if n, err := bdb.Expire(key, khash, 10); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}

			if tp, err := bdb.Type(key, khash); err != nil {
				t.Fatal(err)
			} else if tp != dt {
				t.Fatalf("type fail exp:%s act:%s", dt, tp)
			}

			if n, err := bdb.PTTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n <= 9000 || n >= 10000 {
				t.Fatal(n)
			}

			if n, err := bdb.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 10 {
				t.Fatal(n)
			}

			if n, err := bdb.Persist(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}

			if n, err := bdb.PExpire(key, khash, 990); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}

			if n, err := bdb.PTTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 990 {
				t.Fatal(n)
			}
			if n, err := bdb.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}

			when := tclock.GetTimestampSecond() + 5
			if n, err := bdb.ExpireAt(key, khash, when); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}
			if n, err := bdb.PTTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n <= 4000 || n >= 5000 {
				t.Fatal(n)
			}
			if n, err := bdb.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 5 {
				t.Fatal(n)
			}

			when = tclock.GetTimestampMilli() + 1900
			if n, err := bdb.PExpireAt(key, khash, when); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}
			if n, err := bdb.PTTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 1900 {
				t.Fatal(n)
			}
			if n, err := bdb.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 2 {
				t.Fatal(n)
			}

			if tp, err := bdb.Type(key, khash); err != nil {
				t.Fatal(err)
			} else if tp != dt {
				t.Fatalf("type fail exp:%s act:%s", dt, tp)
			}
		}

		checkValue := func(key []byte, khash uint32, value []byte) {
			if v, vcloser, err := bdb.Get(key, khash); err != nil {
				t.Fatal(err)
			} else if !bytes.Equal(v, value) {
				t.Fatal("string val not eq", v, value)
			} else {
				if vcloser != nil {
					vcloser()
				}
			}
		}

		checkPersist := func(key []byte, khash uint32) {
			if n, err := bdb.Persist(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}
			if n, err := bdb.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != bitsdb.ErrnoKeyPersist {
				t.Fatal(n)
			}
		}

		checkSet := func(key []byte, khash uint32, value []byte) {
			if err := bdb.Set(key, khash, value); err != nil {
				t.Fatal(err)
			}
			if n, err := bdb.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != bitsdb.ErrnoKeyPersist {
				t.Fatal(n)
			}
		}

		key := []byte("string_persist_test_key")
		val := []byte("string_persist_test_val")
		khash := hash.Fnv32(key)
		checkSet(key, khash, val)
		checkKey(key, khash, btools.DataTypeStringName)
		checkValue(key, khash, val)
		checkPersist(key, khash)
		checkValue(key, khash, val)
		checkSet(key, khash, []byte("string_persist_test_val123"))

		key = []byte("hash_persist_test")
		khash = hash.Fnv32(key)
		if n, err := bdb.HSet(key, khash, []byte("field"), []byte{}); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		checkKey(key, khash, btools.DataTypeHashName)
		if n, err := bdb.HLen(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		key = []byte("list_persist_test")
		khash = hash.Fnv32(key)
		if n, err := bdb.LPush(key, khash, []byte("field")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		checkKey(key, khash, btools.DataTypeListName)
		if n, err := bdb.LLen(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		key = []byte("set_persist_test")
		khash = hash.Fnv32(key)
		if n, err := bdb.SAdd(key, khash, []byte("field")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		checkKey(key, khash, btools.DataTypeSetName)
		if n, err := bdb.SCard(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		key = []byte("zset_persist_test")
		khash = hash.Fnv32(key)
		if n, err := bdb.ZAdd(key, khash, getFloatByte(1), []byte("a")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		checkKey(key, khash, btools.DataTypeZsetName)
		if n, err := bdb.ZCard(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
	}
}

func TestKeys_Expire_Dels(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db
		setExpire := func(key []byte, khash uint32, duration int64) {
			n, err := bdb.Expire(key, khash, duration)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
		}
		setExpireAt := func(key []byte, khash uint32, duration int64) {
			n, err := bdb.ExpireAt(key, khash, duration)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
		}
		setPExpire := func(key []byte, khash uint32, duration int64) {
			n, err := bdb.PExpire(key, khash, duration)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
		}
		setPExpireAt := func(key []byte, khash uint32, duration int64) {
			n, err := bdb.PExpireAt(key, khash, duration)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
		}

		times := make([]int64, 10)
		for i := 0; i < 10; i++ {
			stringKey := []byte(fmt.Sprintf("string_del_key_%d", i))
			stringValue := []byte(fmt.Sprintf("string_del_value_%d", i))
			hkey := []byte(fmt.Sprintf("hash_del_key_%d", i))
			hfield := []byte(fmt.Sprintf("hash_del_field_%d", i))
			hvalue := []byte(fmt.Sprintf("hash_del_value_%d", i))
			skey := []byte(fmt.Sprintf("set_del_key_%d", i))
			sfield := []byte(fmt.Sprintf("set_del_field_%d", i))
			zkey := []byte(fmt.Sprintf("zset_del_key_%d", i))
			zfield := []byte(fmt.Sprintf("zset_del_field_%d", i))
			lkey := []byte(fmt.Sprintf("list_del_key_%d", i))
			lfield := []byte(fmt.Sprintf("list_del_lfield_%d", i))
			lrfield := []byte(fmt.Sprintf("list_del_lrfield_%d", i))
			stkhash := hash.Fnv32(stringKey)
			hkhash := hash.Fnv32(hkey)
			skhash := hash.Fnv32(skey)
			zkhash := hash.Fnv32(zkey)
			lkhash := hash.Fnv32(lkey)

			require.NoError(t, bdb.Set(stringKey, stkhash, stringValue))

			n, err := bdb.HSet(hkey, hkhash, hfield, hvalue)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)

			n, err = bdb.SAdd(skey, skhash, sfield)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)

			n, err = bdb.ZAdd(zkey, zkhash, getFloatByte(1), zfield)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)

			n, err = bdb.LPush(lkey, lkhash, lfield)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)

			n, err = bdb.RPush(lkey, lkhash, lrfield)
			require.NoError(t, err)
			require.Equal(t, int64(2), n)

			if i == 1 {
				setExpire(stringKey, stkhash, 1)
				setExpire(hkey, hkhash, 1)
				setPExpire(zkey, zkhash, 900)
			} else if i >= 7 {
				times[i] = tclock.GetTimestampSecond()
				setExpire(stringKey, stkhash, 100)
				setExpire(hkey, hkhash, 100)
				setPExpire(zkey, zkhash, 100000)
				setExpireAt(skey, skhash, times[i]+100)
				setPExpireAt(lkey, lkhash, times[i]*1000+100000)
			}
		}

		time.Sleep(1 * time.Second)

		for i := 0; i < 10; i++ {
			stringKey := []byte(fmt.Sprintf("string_del_key_%d", i))
			hkey := []byte(fmt.Sprintf("hash_del_key_%d", i))
			skey := []byte(fmt.Sprintf("set_del_key_%d", i))
			zkey := []byte(fmt.Sprintf("zset_del_key_%d", i))
			lkey := []byte(fmt.Sprintf("list_del_key_%d", i))
			stkhash := hash.Fnv32(stringKey)
			hkhash := hash.Fnv32(hkey)
			skhash := hash.Fnv32(skey)
			zkhash := hash.Fnv32(zkey)
			lkhash := hash.Fnv32(lkey)

			var n, num, listNum, ttlRet int64
			if i < 5 {
				var delKeys [][]byte
				var expDelNum int64
				delKeys = append(delKeys, lkey, zkey, skey, stringKey, hkey, []byte(fmt.Sprintf("none_del_key_%d", i)))
				delNum, err := bdb.Del(hash.Fnv32(lkey), delKeys...)
				require.NoError(t, err)
				if i == 1 {
					expDelNum = 2
				} else {
					expDelNum = 5
				}
				require.Equal(t, expDelNum, delNum)

				ttlRet = bitsdb.ErrnoKeyNotFoundOrExpire
			} else if i == 7 {
				setExpire(stringKey, stkhash, -1)
				setExpire(hkey, hkhash, 0)
				setPExpire(zkey, zkhash, -100)
				setExpireAt(skey, skhash, tclock.GetTimestampSecond()-100)
				setPExpireAt(lkey, lkhash, -100000)

				ttlRet = bitsdb.ErrnoKeyNotFoundOrExpire
			} else {
				num = 1
				listNum = 2
				ttlRet = bitsdb.ErrnoKeyPersist
				if i > 7 {
					ttlRet = tclock.GetTimestampSecond() - times[i] - 2
				}
			}

			v, vcloser, err := bdb.Get(stringKey, stkhash)
			require.NoError(t, err)
			if i < 5 || i == 7 {
				require.Equal(t, []byte(nil), v)
			} else {
				require.Equal(t, []byte(fmt.Sprintf("string_del_value_%d", i)), v)
				vcloser()
			}

			n, err = bdb.HLen(hkey, hkhash)
			require.NoError(t, err)
			require.Equal(t, num, n)

			n, err = bdb.TTL(hkey, hkhash)
			require.NoError(t, err)
			if n < ttlRet-1 {
				t.Fatal(i, n, ttlRet-1)
			}

			n, err = bdb.SCard(skey, skhash)
			require.NoError(t, err)
			require.Equal(t, num, n)

			if n, err = bdb.TTL(skey, skhash); err != nil {
				t.Fatal(err)
			} else if n < ttlRet-1 {
				t.Fatal(i, n, ttlRet-1)
			}

			n, err = bdb.ZCard(zkey, zkhash)
			require.NoError(t, err)
			require.Equal(t, num, n)

			if n, err = bdb.TTL(zkey, zkhash); err != nil {
				t.Fatal(err)
			} else if n < ttlRet-1 {
				t.Fatal(i, n, ttlRet-1)
			}

			n, err = bdb.LLen(lkey, lkhash)
			require.NoError(t, err)
			require.Equal(t, listNum, n)

			if n, err = bdb.TTL(lkey, lkhash); err != nil {
				t.Fatal(err)
			} else if n < ttlRet-1 {
				t.Fatal(i, n, ttlRet-1)
			}
		}
	}
}

func TestKeys_Expire(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("TestKeys_CheckExpire_string_key")
		khash := hash.Fnv32(key)
		val := []byte("TestKeys_CheckExpire_string")
		if err := bdb.Set(key, khash, val); err != nil {
			t.Fatal("Set err", string(key), err)
		}

		setkey := []byte("TestKeys_CheckExpire_set_key")
		setkhash := hash.Fnv32(setkey)
		setmember := []byte("TestKeys_CheckExpire_set_member")
		if n, err := bdb.SAdd(setkey, setkhash, setmember); err != nil {
			t.Fatal("Set err", string(setkey), err)
		} else if n != 1 {
			t.Fatal("Set return n err", string(setkey), n)
		}

		hkey := []byte("TestKeys_CheckExpire_hash_key")
		hkhash := hash.Fnv32(hkey)
		hfield := []byte("TestKeys_CheckExpire_hash_field")
		hvalue := []byte("TestKeys_CheckExpire_hash_value")
		if n, err := bdb.HSet(hkey, hkhash, hfield, hvalue); err != nil {
			t.Fatal("HSet err", string(hkey), err)
		} else if n != 1 {
			t.Fatal("HSet return n err", string(hkey), n)
		}

		zkey := []byte("TestKeys_CheckExpire_zset_key")
		zkhash := hash.Fnv32(zkey)
		zfield := []byte("TestKeys_CheckExpire_zset_field")
		if n, err := bdb.ZAdd(zkey, zkhash, getFloatByte(1), zfield); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		lkey := []byte("TestKeys_CheckExpire_list_key")
		lkhash := hash.Fnv32(lkey)
		lfield := []byte("TestKeys_CheckExpire_list_lfield")
		lrfield := []byte("TestKeys_CheckExpire_list_lrfield")
		if n, err := bdb.LPush(lkey, lkhash, lfield); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.RPush(lkey, lkhash, lrfield); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		exist, _ := bdb.bitsdb.Exists(key, khash)
		require.Equal(t, int64(1), exist)
		exist, _ = bdb.bitsdb.Exists(setkey, setkhash)
		require.Equal(t, int64(1), exist)
		exist, _ = bdb.bitsdb.Exists(hkey, hkhash)
		require.Equal(t, int64(1), exist)
		exist, _ = bdb.bitsdb.Exists(zkey, zkhash)
		require.Equal(t, int64(1), exist)
		exist, _ = bdb.bitsdb.Exists(lkey, lkhash)
		require.Equal(t, int64(1), exist)

		if n, err := bdb.Expire(key, khash, 1); err != nil {
			t.Fatal("Expire err", string(key), err)
		} else if n != 1 {
			t.Fatal("Expire return n err", string(key), n)
		}
		if n, err := bdb.Expire(setkey, setkhash, 1); err != nil {
			t.Fatal("Expire err", string(setkey), err)
		} else if n != 1 {
			t.Fatal("Expire return n err", string(setkey), n)
		}
		if n, err := bdb.Expire(hkey, hkhash, 1); err != nil {
			t.Fatal("Expire err", string(hkey), err)
		} else if n != 1 {
			t.Fatal("Expire return n err", string(hkey), n)
		}
		if n, err := bdb.Expire(zkey, zkhash, 1); err != nil {
			t.Fatal("Expire err", string(zkey), err)
		} else if n != 1 {
			t.Fatal("Expire return n err", string(zkey), n)
		}
		if n, err := bdb.Expire(lkey, lkhash, 1); err != nil {
			t.Fatal("Expire err", string(lkey), err)
		} else if n != 1 {
			t.Fatal("Expire return n err", string(lkey), n)
		}

		testCheckKeyValue(t, bdb, key, khash, val)
		if n, err := bdb.SCard(setkey, setkhash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.HLen(hkey, hash.Fnv32(hkey)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.ZCard(zkey, hash.Fnv32(zkey)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.LLen(lkey, hash.Fnv32(lkey)); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		bdb.bitsdb.ClearCache()
		time.Sleep(time.Second)

		exist, _ = bdb.bitsdb.Exists(key, khash)
		require.Equal(t, int64(0), exist)
		exist, _ = bdb.bitsdb.Exists(setkey, setkhash)
		require.Equal(t, int64(0), exist)
		exist, _ = bdb.bitsdb.Exists(hkey, hkhash)
		require.Equal(t, int64(0), exist)
		exist, _ = bdb.bitsdb.Exists(zkey, zkhash)
		require.Equal(t, int64(0), exist)
		exist, _ = bdb.bitsdb.Exists(lkey, lkhash)
		require.Equal(t, int64(0), exist)
	}
}

func TestKeys_WrongType(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("TestKeys_WrongType_key")
		khash := hash.Fnv32(key)
		sfield := []byte("TestKeys_WrongType_set_field")
		hfield := []byte("TestKeys_WrongType_hash_field")
		hvalue := []byte("TestKeys_WrongType_hash_value")
		llfield := []byte("TestKeys_WrongType_list_llfield")
		lrfield := []byte("TestKeys_WrongType_list_lrfield")
		zfield := []byte("TestKeys_WrongType_zset_field")

		checkErrWrongType := func(dt uint8) {
			if dt != btools.DataTypeSet {
				_, err := bdb.SAdd(key, khash, sfield)
				require.Equal(t, bitsdb.ErrWrongType, err)
			}
			if dt != btools.DataTypeZset {
				_, err := bdb.ZAdd(key, khash, getFloatByte(1), zfield)
				require.Equal(t, bitsdb.ErrWrongType, err)
			}
			if dt != btools.DataTypeList {
				_, err := bdb.LPush(key, khash, llfield)
				require.Equal(t, bitsdb.ErrWrongType, err)
				_, err = bdb.RPush(key, khash, lrfield)
				require.Equal(t, bitsdb.ErrWrongType, err)
			}
			if dt != btools.DataTypeHash {
				_, err := bdb.HSet(key, khash, hfield, hvalue)
				require.Equal(t, bitsdb.ErrWrongType, err)
				err = bdb.HMset(key, khash, hfield, hvalue)
				require.Equal(t, bitsdb.ErrWrongType, err)
			}
		}

		require.NoError(t, bdb.Set(key, khash, key))
		checkErrWrongType(btools.DataTypeString)

		if n, err := bdb.Del(khash, key); err != nil {
			t.Fatal("Del err", err)
		} else if n != 1 {
			t.Fatal("Del return n err", n)
		}

		if n, err := bdb.HSet(key, khash, hfield, hvalue); err != nil {
			t.Fatal("HSet err", err)
		} else if n != 1 {
			t.Fatal("HSet return n err", n)
		}
		if n, err := bdb.HLen(key, khash); err != nil {
			t.Fatal("HLen err", err)
		} else if n != 1 {
			t.Fatal("HLen return n err", n)
		}
		checkErrWrongType(btools.DataTypeHash)

		if n, err := bdb.Del(khash, key); err != nil {
			t.Fatal("Del err", err)
		} else if n != 1 {
			t.Fatal("Del return n err", n)
		}

		if n, err := bdb.LPush(key, khash, llfield); err != nil {
			t.Fatal("LPush err", err)
		} else if n != 1 {
			t.Fatal("LPush return n err", n)
		}
		if n, err := bdb.RPush(key, khash, lrfield); err != nil {
			t.Fatal("RPush err", err)
		} else if n != 2 {
			t.Fatal("RPush return n err", n)
		}
		if n, err := bdb.LLen(key, khash); err != nil {
			t.Fatal("LLen err", err)
		} else if n != 2 {
			t.Fatal("LLen return n err", n)
		}
		checkErrWrongType(btools.DataTypeList)

		if n, err := bdb.Expire(key, khash, 1); err != nil {
			t.Fatal("Expire err", err)
		} else if n != 1 {
			t.Fatal("Expire return n err", n)
		}
		time.Sleep(time.Second)

		if n, err := bdb.ZAdd(key, khash, getFloatByte(1), zfield); err != nil {
			t.Fatal("Zadd err", err)
		} else if n != 1 {
			t.Fatal("Zadd return n err", n)
		}
		if n, err := bdb.ZCard(key, khash); err != nil {
			t.Fatal("ZCard err", err)
		} else if n != 1 {
			t.Fatal("ZCard return n err", n)
		}
		checkErrWrongType(btools.DataTypeZset)

		if n, err := bdb.Expire(key, khash, 1); err != nil {
			t.Fatal("Expire err", err)
		} else if n != 1 {
			t.Fatal("Expire return n err", n)
		}
		time.Sleep(time.Second)

		if n, err := bdb.SAdd(key, khash, sfield); err != nil {
			t.Fatal("SAdd err", err)
		} else if n != 1 {
			t.Fatal("SAdd return n err", n)
		}
		if n, err := bdb.SCard(key, khash); err != nil {
			t.Fatal("SCard err", err)
		} else if n != 1 {
			t.Fatal("SCard return n err", n)
		}
		checkErrWrongType(btools.DataTypeSet)
	}
}

//func TestKeys_ScanBySlotId(t *testing.T) {
//	cores := testTwoBitsCores()
//	defer closeCores(cores)
//
//	for _, cr := range cores {
//		bdb := cr.db
//		var keys []string
//		slotId := uint32(1)
//		count := 10000
//		sfield := []byte("TestKeys_set_field")
//		hfield := []byte("TestKeys_hash_field")
//		hvalue := []byte("TestKeys_hash_value")
//		llfield := []byte("TestKeys_list_llfield")
//		zfield := []byte("TestKeys_zset_field")
//
//		index := 0
//		for {
//			k := fmt.Sprintf("TestScanBySlotIdKey_%d", index)
//			index++
//
//			if uint32(utils.GetSlotId(hash.Fnv32([]byte(k)))) != slotId {
//				continue
//			}
//
//			keys = append(keys, k)
//			if len(keys) == count {
//				break
//			}
//		}
//
//		sort.Strings(keys)
//
//		for i := 0; i < count; i++ {
//			key := []byte(keys[i])
//			khash := hash.Fnv32(key)
//			switch i % 5 {
//			case 0:
//				if err := bdb.Set(key, khash, key); err != nil {
//					t.Fatal("Set err", err)
//				}
//			case 1:
//				if n, err := bdb.SAdd(key, khash, sfield); err != nil {
//					t.Fatal("SAdd err", err)
//				} else if n != 1 {
//					t.Fatal("SAdd return n err", n)
//				}
//			case 2:
//				if n, err := bdb.ZAdd(key, khash, getFloatByte(1), zfield); err != nil {
//					t.Fatal("Zadd err", err)
//				} else if n != 1 {
//					t.Fatal("Zadd return n err", n)
//				}
//			case 3:
//				if n, err := bdb.HSet(key, khash, hfield, hvalue); err != nil {
//					t.Fatal("HSet err", err)
//				} else if n != 1 {
//					t.Fatal("HSet return n err", n)
//				}
//			case 4:
//				if n, err := bdb.LPush(key, khash, llfield); err != nil {
//					t.Fatal("LPush err", err)
//				} else if n != 1 {
//					t.Fatal("LPush return n err", n)
//				}
//			}
//		}
//
//		limit := 1000
//		cnt := 0
//		var err error
//		var cursor []byte
//		var scanList []btools.ScanPair
//		var expDt btools.DataType
//
//		for {
//			cursor, scanList, err = bdb.ScanBySlotId(slotId, cursor, limit, "*")
//			require.NoError(t, err)
//
//			for i := range scanList {
//				scanKey := []byte(keys[cnt])
//				require.Equal(t, scanKey, scanList[i].Key)
//
//				dt := scanList[i].Dt
//				switch cnt % 5 {
//				case 0:
//					expDt = btools.DtString
//				case 1:
//					expDt = btools.DtSet
//				case 2:
//					expDt = btools.DtZset
//				case 3:
//					expDt = btools.DtHash
//				case 4:
//					expDt = btools.DtList
//				}
//				require.Equal(t, expDt, dt)
//				cnt++
//			}
//
//			if len(scanList) < limit || bytes.Equal(cursor, btools.ScanEndCurosr) {
//				break
//			}
//		}
//
//		require.Equal(t, count, cnt)
//	}
//}
