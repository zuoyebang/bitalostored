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
	"testing"
	"time"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/butils/numeric"
)

func TestDB_ScanExpireData(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		jobId := uint64(0)
		keyKind := base.KeyKindFieldCompress
		keyVerMap := make(map[string]uint64)
		setKeyVerMap := func(k []byte) {
			if _, ok := keyVerMap[string(k)]; !ok {
				keyIdCur := bdb.ZsetObj.GetCurrentKeyId()
				keyVerMap[string(k)] = base.EncodeKeyVersion(keyIdCur, keyKind)
			}
		}

		strkeyStale := []byte("string_stale")
		strkeyStaleVal := []byte("string_stale_value")
		strkeyStaleHash := hash.Fnv32(strkeyStale)
		if err := bdb.StringObj.Set(strkeyStale, strkeyStaleHash, strkeyStaleVal); err != nil {
			t.Fatal(err)
		}

		skeyStale := []byte("set_stale")
		skeyStaleField := []byte("set_stale_field")
		skeyStaleHash := hash.Fnv32(skeyStale)
		if n, err := bdb.SetObj.SAdd(skeyStale, skeyStaleHash, skeyStaleField); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		zskeyStale := []byte("zset_stale")
		zskeyStaleField := []byte("zset_stale_field")
		zskeyStaleHash := hash.Fnv32(zskeyStale)
		if n, err := bdb.ZsetObj.ZAdd(zskeyStale, zskeyStaleHash, btools.ScorePair{Score: 10, Member: zskeyStaleField}); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		opKey := func(i int, key []byte, khash uint32) {
			if i >= 0 && i < 10 {
				if n, err := bdb.StringObj.Expire(key, khash, 3); err != nil {
					t.Fatal(err)
				} else if n != 1 {
					t.Fatal(n)
				}
			} else if i >= 10 && i < 20 {
				if n, err := bdb.StringObj.ExpireAt(key, khash, tclock.GetTimestampSecond()+3); err != nil {
					t.Fatal(err)
				} else if n != 1 {
					t.Fatal(n)
				}
			} else if i >= 20 && i < 30 {
				if n, err := bdb.StringObj.Del(khash, key); err != nil {
					t.Fatal(err)
				} else if n != 1 {
					t.Fatal(n)
				}
			} else if i >= 30 && i < 40 {
				if n, err := bdb.StringObj.Expire(key, khash, 0); err != nil {
					t.Fatal(err)
				} else if n != 1 {
					t.Fatal(n)
				}
			} else if i >= 40 && i < 50 {
				if n, err := bdb.StringObj.Expire(key, khash, 3); err != nil {
					t.Fatal(err)
				} else if n != 1 {
					t.Fatal(n)
				}
				if n, err := bdb.StringObj.BasePersist(key, khash); err != nil {
					t.Fatal(err)
				} else if n != 1 {
					t.Fatal(n)
				}
			} else if i >= 50 && i < 60 {
				if n, err := bdb.StringObj.Expire(key, khash, 100); err != nil {
					t.Fatal(err)
				} else if n != 1 {
					t.Fatal(n)
				}
			}
		}

		for i := 0; i < 100; i++ {
			strkey := []byte(fmt.Sprintf("string_key_%d", i))
			strkeyHash := hash.Fnv32(strkey)
			strkeyVal := []byte(fmt.Sprintf("string_key_value_%d", i))
			if err := bdb.StringObj.Set(strkey, strkeyHash, strkeyVal); err != nil {
				t.Fatal(err)
			}
			opKey(i, strkey, strkeyHash)

			skey := []byte(fmt.Sprintf("set_key_%d", i))
			skeyHash := hash.Fnv32(skey)
			for j := 0; j < 100; j++ {
				skeyField := []byte(fmt.Sprintf("set_field_%d_%d", i, j))
				if n, err := bdb.SetObj.SAdd(skey, skeyHash, skeyField); err != nil {
					t.Fatal(err)
				} else if n != 1 {
					t.Fatal(n)
				}
			}
			opKey(i, skey, skeyHash)

			zskey := []byte(fmt.Sprintf("zset_key_%d", i))
			zskeyHash := hash.Fnv32(zskey)
			for j := 0; j < 100; j++ {
				zskeyField := []byte(fmt.Sprintf("zset_field_%d_%d", i, j))
				if n, err := bdb.ZsetObj.ZAdd(zskey, zskeyHash, btools.ScorePair{Score: float64(j), Member: zskeyField}); err != nil {
					t.Fatal(err)
				} else if n != 1 {
					t.Fatal(n)
				}

				if j == 0 {
					setKeyVerMap(zskey)
				}
			}
			opKey(i, zskey, zskeyHash)
		}

		for i := 0; i < 100; i++ {
			strkey := []byte(fmt.Sprintf("string_key_%d", i))
			if i >= 20 && i < 40 {
				testCheckKeyValue(t, bdb, strkey, hash.Fnv32(strkey), nil)
			} else {
				strkeyVal := []byte(fmt.Sprintf("string_key_value_%d", i))
				testCheckKeyValue(t, bdb, strkey, hash.Fnv32(strkey), strkeyVal)
			}

			skey := []byte(fmt.Sprintf("set_key_%d", i))
			if n, err := bdb.SetObj.SCard(skey, hash.Fnv32(skey)); err != nil {
				t.Fatal(err)
			} else if i >= 20 && i < 40 {
				if n != 0 {
					t.Fatalf("scard exp=0, key=%s, n=%d", string(skey), n)
				}
			} else if n != 100 {
				t.Fatalf("scard exp=100, key=%s, n=%d", string(skey), n)
			}

			zskey := []byte(fmt.Sprintf("zset_key_%d", i))
			if n, err := bdb.ZsetObj.ZCard(zskey, hash.Fnv32(zskey)); err != nil {
				t.Fatal(err)
			} else if i >= 20 && i < 40 {
				if n != 0 {
					t.Fatalf("zcard exp=0, key=%s, n=%d", string(zskey), n)
				}
			} else if n != 100 {
				t.Fatalf("zcard exp=100, key=%s, n=%d", string(zskey), n)
			}
		}

		time.Sleep(3 * time.Second)

		checkDataDbNum := func(expNum int) {
			setDataDbNum := 0
			setDataIt := bdb.SetObj.DataDb.NewIterator(nil)
			for setDataIt.First(); setDataIt.Valid(); setDataIt.Next() {
				setDataDbNum++
			}
			setDataIt.Close()
			require.Equal(t, expNum, setDataDbNum)
		}

		checkIndexDbNum := func(expNum int) {
			zsetIndexDbNum := 0
			zsIndexIt := bdb.ZsetObj.DataDb.NewIteratorIndex(nil)
			for zsIndexIt.First(); zsIndexIt.Valid(); zsIndexIt.Next() {
				zsetIndexDbNum++
			}
			require.Equal(t, expNum, zsetIndexDbNum)
			zsIndexIt.Close()
		}

		checkDataDbNum(100*100 + 1)
		checkIndexDbNum(100*100 + 1)

		bdb.ScanDeleteExpireDb(jobId)
		require.Equal(t, uint64(80), bdb.delExpireStat.metaDbKeys.Load())
		require.Equal(t, uint64(80), bdb.delExpireStat.expireDbKeys.Load())
		require.Equal(t, uint64(4000), bdb.delExpireStat.zsetDataDbKeys.Load())
		require.Equal(t, uint64(4000), bdb.delExpireStat.zsetIndexDbKeys.Load())

		checkDataDbNum(100*100 + 1)
		checkIndexDbNum(100*60 + 1)

		bdb.FlushAllDB()

		checkDataDbNum(100*60 + 1)

		var keepNum, delNum int
		for i := 0; i < 100; i++ {
			skey := []byte(fmt.Sprintf("set_key_%d", i))
			setMkv, err := bdb.SetObj.GetMetaDataCheckAlive(skey, hash.Fnv32(skey))
			if err != nil {
				t.Fatal(err)
			}
			if setMkv != nil {
				if i >= 40 {
					keepNum++
				}
			} else {
				delNum++
			}
		}
		require.Equal(t, 40, delNum)
		require.Equal(t, 60, keepNum)

		delNum = 0
		keepNum = 0
		for i := 0; i < 100; i++ {
			zskey := []byte(fmt.Sprintf("zset_key_%d", i))
			zsetMkv, err := bdb.ZsetObj.GetMetaDataCheckAlive(zskey, hash.Fnv32(zskey))
			if err != nil {
				t.Fatal(err)
			}
			if zsetMkv != nil {
				if i >= 40 {
					keepNum++
				}
			} else {
				delNum++
			}
		}
		require.Equal(t, 40, delNum)
		require.Equal(t, 60, keepNum)

		delNum = 0
		keepNum = 0
		for i := 0; i < 100; i++ {
			strkey := []byte(fmt.Sprintf("string_key_%d", i))
			strVal, strValCloser, err := bdb.StringObj.Get(strkey, hash.Fnv32(strkey))
			if err != nil {
				t.Fatal(err)
			}
			if strVal != nil {
				if i >= 40 {
					keepNum++
				}
			} else {
				delNum++
			}
			if strValCloser != nil {
				strValCloser()
			}
		}
		require.Equal(t, 40, delNum)
		require.Equal(t, 60, keepNum)

		for i := 0; i < 100; i++ {
			skey := []byte(fmt.Sprintf("set_key_%d", i))
			skeyHash := hash.Fnv32(skey)
			count, err := bdb.SetObj.SCard(skey, skeyHash)
			require.NoError(t, err)
			if i < 40 {
				require.Equal(t, count, int64(0))
			} else {
				require.Equal(t, count, int64(100))
			}
		}
		if n, err := bdb.SetObj.SCard(skeyStale, skeyStaleHash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal("skeyStale scard err", n)
		}

		for i := 0; i < 100; i++ {
			zskey := []byte(fmt.Sprintf("zset_key_%d", i))
			zskeyVer := keyVerMap[string(zskey)]
			zskeyHash := hash.Fnv32(zskey)
			count, err := bdb.ZsetObj.ZCard(zskey, zskeyHash)
			require.NoError(t, err)
			if i < 40 {
				require.Equal(t, count, int64(0))
				for j := 0; j < 100; j++ {
					zskeyField := []byte(fmt.Sprintf("zset_field_%d_%d", i, j))
					var dataKey [base.DataKeyZsetLength]byte
					base.EncodeZsetDataKey(dataKey[:], zskeyVer, zskeyHash, zskeyField)
					_, dataValExist, _, err := bdb.ZsetObj.GetDataValue(dataKey[:])
					require.NoError(t, err)
					if dataValExist {
						t.Fatal("zset notexist key dataKey found", string(zskey), zskeyField)
					}
				}
			} else {
				require.Equal(t, count, int64(100))
				for j := 0; j < 100; j++ {
					zskeyField := []byte(fmt.Sprintf("zset_field_%d_%d", i, j))
					var dataKey [base.DataKeyZsetLength]byte
					base.EncodeZsetDataKey(dataKey[:], zskeyVer, zskeyHash, zskeyField)
					dataVal, dataValExist, dataValCloser, err := bdb.ZsetObj.GetDataValue(dataKey[:])
					require.NoError(t, err)
					if !dataValExist {
						t.Fatal("zset dataKey not found", string(zskey), string(zskeyField))
					}
					dataScore := numeric.ByteSortToFloat64(dataVal)
					if dataScore != float64(j) {
						t.Fatal("zset dataKey score not eq", string(zskey), string(zskeyField), dataScore, j)
					}
					if dataValCloser != nil {
						dataValCloser()
					}
				}
			}
		}
		if n, err := bdb.ZsetObj.ZCard(zskeyStale, zskeyStaleHash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal("zskeyStale scard err", n)
		}
	}
}
