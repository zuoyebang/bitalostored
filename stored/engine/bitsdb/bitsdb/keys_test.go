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
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

func testGetMetaDataByKey(b *BitsDB, key []byte, khash uint32) (*base.MetaData, error) {
	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	return b.baseDb.BaseGetMetaWithoutValue(mk)
}

func testIsExistExpire(b *BitsDB, key []byte, mkv *base.MetaData) bool {
	if mkv == nil {
		return false
	}
	expEk, expEkCloser := base.EncodeExpireKey(key, mkv)
	defer expEkCloser()
	isExist, _ := b.baseDb.DB.IsExistExpire(expEk)
	return isExist
}

func testCheckExpireCmdExpireKey(t *testing.T, b *BitsDB, key []byte, khash uint32) {
	oldMkv, _ := testGetMetaDataByKey(b, key, khash)
	if oldMkv.GetDataType() == btools.STRING {
		return
	}

	if n, err := b.StringObj.Expire(key, khash, 100); err != nil {
		t.Fatal("Expire err", string(key), err)
	} else if n != 1 {
		t.Fatal("Expire return n err", string(key), n)
	}
	if testIsExistExpire(b, key, oldMkv) {
		t.Fatal("old expireKey exist", string(key))
	}
	newMkv, _ := testGetMetaDataByKey(b, key, khash)
	if !testIsExistExpire(b, key, newMkv) {
		t.Fatal("new expireKey not exist", string(key))
	}
}

func testCheckDelCmdExpireKey(t *testing.T, b *BitsDB, key []byte, khash uint32) {
	oldMkv, _ := testGetMetaDataByKey(b, key, khash)
	if oldMkv.GetDataType() == btools.STRING {
		return
	}

	if !testIsExistExpire(b, key, oldMkv) {
		t.Fatal("old expireKey not exist", string(key))
	}
	if n, err := b.StringObj.Del(khash, key); err != nil {
		t.Fatal("Expire err", string(key), err)
	} else if n != 1 {
		t.Fatal("Expire return n err", string(key), n)
	}
	if testIsExistExpire(b, key, oldMkv) {
		t.Fatal("old expireKey exist", string(key))
	}
}

func TestKeys_Expire_Persist_TTL_Type(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		checkKey := func(key []byte, khash uint32, dt string) {
			if n, err := bdb.StringObj.BasePersist(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 0 {
				t.Fatal(n)
			}

			if tp, err := bdb.StringObj.Type(key, khash); err != nil {
				t.Fatal(err)
			} else if tp != dt {
				t.Fatalf("type fail exp:%s act:%s", dt, tp)
			}

			if n, err := bdb.StringObj.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != base.ErrnoKeyPersist {
				t.Fatal(n)
			}

			if n, err := bdb.StringObj.Expire(key, khash, 10); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}

			oldMkv, _ := testGetMetaDataByKey(bdb, key, khash)

			if tp, err := bdb.StringObj.Type(key, khash); err != nil {
				t.Fatal(err)
			} else if tp != dt {
				t.Fatalf("type fail exp:%s act:%s", dt, tp)
			}

			if n, err := bdb.StringObj.PTTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n <= 9000 || n >= 10000 {
				t.Fatal(n)
			}

			if n, err := bdb.StringObj.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 10 {
				t.Fatal(n)
			}

			if n, err := bdb.StringObj.BasePersist(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}

			if n, err := bdb.StringObj.PExpire(key, khash, 990); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}

			if dt != btools.StringName {
				if testIsExistExpire(bdb, key, oldMkv) {
					t.Fatal("old expireKey exist", string(key))
				}
				newMkv, _ := testGetMetaDataByKey(bdb, key, khash)
				if !testIsExistExpire(bdb, key, newMkv) {
					t.Fatal("new expireKey not exist", string(key))
				}
			}

			if n, err := bdb.StringObj.PTTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 990 {
				t.Fatal(n)
			}
			if n, err := bdb.StringObj.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}

			when := tclock.GetTimestampSecond() + 5
			if n, err := bdb.StringObj.ExpireAt(key, khash, when); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}
			if n, err := bdb.StringObj.PTTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n <= 4000 || n >= 5000 {
				t.Fatal(n)
			}
			if n, err := bdb.StringObj.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 5 {
				t.Fatal(n)
			}

			when = tclock.GetTimestampMilli() + 1900
			if n, err := bdb.StringObj.PExpireAt(key, khash, when); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}
			if n, err := bdb.StringObj.PTTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 1900 {
				t.Fatal(n)
			}
			if n, err := bdb.StringObj.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 2 {
				t.Fatal(n)
			}

			if tp, err := bdb.StringObj.Type(key, khash); err != nil {
				t.Fatal(err)
			} else if tp != dt {
				t.Fatalf("type fail exp:%s act:%s", dt, tp)
			}
		}

		checkValue := func(key []byte, khash uint32, value []byte) {
			if v, vcloser, err := bdb.StringObj.Get(key, khash); err != nil {
				t.Fatal(err)
			} else if !bytes.Equal(v, value) {
				t.Fatal("string val not eq", v, value)
				// } else if vcloser == nil {
				// 	t.Fatal("vcloser return is nil")
			} else {
				if vcloser != nil {
					vcloser()
				}
			}
		}

		checkPersist := func(key []byte, khash uint32) {
			if n, err := bdb.StringObj.BasePersist(key, khash); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}
			if n, err := bdb.StringObj.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != base.ErrnoKeyPersist {
				t.Fatal(n)
			}
		}

		checkSet := func(key []byte, khash uint32, value []byte) {
			if err := bdb.StringObj.Set(key, khash, value); err != nil {
				t.Fatal(err)
			}
			if n, err := bdb.StringObj.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n != base.ErrnoKeyPersist {
				t.Fatal(n)
			}
		}

		key := []byte("string_persist_test_key")
		val := []byte("string_persist_test_val")
		khash := hash.Fnv32(key)
		checkSet(key, khash, val)
		checkKey(key, khash, btools.StringName)
		checkValue(key, khash, val)
		checkPersist(key, khash)
		checkValue(key, khash, val)
		checkSet(key, khash, []byte("string_persist_test_val123"))

		key = []byte("hash_persist_test")
		khash = hash.Fnv32(key)
		if n, err := bdb.HashObj.HSet(key, khash, []byte("field"), []byte{}); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		checkKey(key, khash, btools.HashName)
		if n, err := bdb.HashObj.HLen(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		key = []byte("list_persist_test")
		khash = hash.Fnv32(key)
		if n, err := bdb.ListObj.LPush(key, khash, []byte("field")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		checkKey(key, khash, btools.ListName)
		if n, err := bdb.ListObj.LLen(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		key = []byte("set_persist_test")
		khash = hash.Fnv32(key)
		if n, err := bdb.SetObj.SAdd(key, khash, []byte("field")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		checkKey(key, khash, btools.SetName)
		if n, err := bdb.SetObj.SCard(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		key = []byte("zset_persist_test")
		khash = hash.Fnv32(key)
		if n, err := bdb.ZsetObj.ZAdd(key, khash, btools.ScorePair{1, []byte("a")}); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		checkKey(key, khash, btools.ZSetName)
		if n, err := bdb.ZsetObj.ZCard(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
	}
}

func TestKeys_Expire_Dels(t *testing.T) {
	for _, isFlush := range []bool{false, true} {
		func() {
			fmt.Println("run isFlush=", isFlush)
			cores := testTwoBitsCores()
			defer closeCores(cores)

			for _, cr := range cores {
				bdb := cr.db

				setExpire := func(key []byte, khash uint32, duration int64) {
					if n, err := bdb.StringObj.Expire(key, khash, duration); err != nil {
						t.Fatal("Expire err", string(key), err)
					} else if n != 1 {
						t.Fatal("Expire return n err", string(key), n)
					}
				}
				setExpireAt := func(key []byte, khash uint32, duration int64) {
					if n, err := bdb.StringObj.ExpireAt(key, khash, duration); err != nil {
						t.Fatal("ExpireAt err", string(key), err)
					} else if n != 1 {
						t.Fatal("ExpireAt return n err", string(key), n)
					}
				}
				setPExpire := func(key []byte, khash uint32, duration int64) {
					if n, err := bdb.StringObj.PExpire(key, khash, duration); err != nil {
						t.Fatal("PExpire err", string(key), err)
					} else if n != 1 {
						t.Fatal("PExpire return n err", string(key), n)
					}
				}
				setPExpireAt := func(key []byte, khash uint32, duration int64) {
					if n, err := bdb.StringObj.PExpireAt(key, khash, duration); err != nil {
						t.Fatal("PExpireAt err", string(key), err)
					} else if n != 1 {
						t.Fatal("PExpireAt return n err", string(key), n)
					}
				}

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

					if err := bdb.StringObj.Set(stringKey, stkhash, stringValue); err != nil {
						t.Fatal(err)
					}
					if n, err := bdb.HashObj.HSet(hkey, hkhash, hfield, hvalue); err != nil {
						t.Fatal(err)
					} else if n != 1 {
						t.Fatal(n)
					}
					if n, err := bdb.SetObj.SAdd(skey, skhash, sfield); err != nil {
						t.Fatal(err)
					} else if n != 1 {
						t.Fatal(n)
					}
					if n, err := bdb.ZsetObj.ZAdd(zkey, zkhash, btools.ScorePair{1, zfield}); err != nil {
						t.Fatal(err)
					} else if n != 1 {
						t.Fatal(n)
					}
					if n, err := bdb.ListObj.LPush(lkey, lkhash, lfield); err != nil {
						t.Fatal(err)
					} else if n != 1 {
						t.Fatal(n)
					}
					if n, err := bdb.ListObj.RPush(lkey, lkhash, lrfield); err != nil {
						t.Fatal(err)
					} else if n != 2 {
						t.Fatal(n)
					}

					if i == 1 {
						setExpire(stringKey, stkhash, 1)
						setExpire(hkey, hkhash, 1)
						setPExpire(zkey, zkhash, 900)
					}

					if i >= 7 {
						setExpire(stringKey, stkhash, 100)
						setExpire(hkey, hkhash, 100)
						setPExpire(zkey, zkhash, 100000)
						setExpireAt(skey, skhash, tclock.GetTimestampSecond()+100)
						setPExpireAt(lkey, lkhash, tclock.GetTimestampMilli()+100000)
					}
				}

				time.Sleep(1 * time.Second)
				if isFlush {
					bdb.FlushAllDB()
				}

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

					var num, listNum, ttlRet int64
					if i < 5 {
						var delKeys [][]byte
						delKeys = append(delKeys, lkey, zkey, skey, stringKey, hkey, []byte(fmt.Sprintf("none_del_key_%d", i)))
						delNum, err := bdb.StringObj.Del(hash.Fnv32(lkey), delKeys...)
						if err != nil {
							t.Fatal(err)
						} else {
							if i == 1 {
								if delNum != 2 {
									t.Fatal(delNum)
								}
							} else {
								if delNum != 5 {
									t.Fatal(delNum)
								}
							}
						}

						num = 0
						listNum = 0
						ttlRet = base.ErrnoKeyNotFoundOrExpire
					} else if i == 7 {
						setExpire(stringKey, stkhash, -1)
						setExpire(hkey, hkhash, 0)
						setPExpire(zkey, zkhash, -100)
						setExpireAt(skey, skhash, tclock.GetTimestampSecond()-100)
						setPExpireAt(lkey, lkhash, -100000)

						num = 0
						ttlRet = base.ErrnoKeyNotFoundOrExpire
					} else {
						num = 1
						listNum = 2
						ttlRet = base.ErrnoKeyPersist
						if i > 7 {
							ttlRet = 96
						}
					}

					v, vcloser, err := bdb.StringObj.Get(stringKey, hash.Fnv32(stringKey))
					if err != nil {
						t.Fatal(err)
					}
					if i < 5 || i == 7 {
						if v != nil {
							t.Fatal("string val del fail", i, string(stringKey))
						}
					} else {
						if !bytes.Equal(v, []byte(fmt.Sprintf("string_del_value_%d", i))) {
							t.Fatal("string val get fail", v)
							// } else if vcloser == nil {
							// 	t.Fatal("vcloser return is nil")
						}
					}
					if vcloser != nil {
						vcloser()
					}

					if n, err := bdb.HashObj.HLen(hkey, hash.Fnv32(hkey)); err != nil {
						t.Fatal(err)
					} else if n != num {
						t.Fatal(n)
					}
					if n, err := bdb.StringObj.TTL(hkey, hash.Fnv32(hkey)); err != nil {
						t.Fatal(err)
					} else if n < ttlRet-1 {
						t.Fatal(n, ttlRet-1)
					}

					if n, err := bdb.SetObj.SCard(skey, hash.Fnv32(skey)); err != nil {
						t.Fatal(err)
					} else if n != num {
						t.Fatal(n)
					}
					if n, err := bdb.StringObj.TTL(skey, hash.Fnv32(skey)); err != nil {
						t.Fatal(err)
					} else if n < ttlRet-1 {
						t.Fatal(n, ttlRet-1)
					}

					if n, err := bdb.ZsetObj.ZCard(zkey, hash.Fnv32(zkey)); err != nil {
						t.Fatal(err)
					} else if n != num {
						t.Fatal(n)
					}
					if n, err := bdb.StringObj.TTL(zkey, hash.Fnv32(zkey)); err != nil {
						t.Fatal(err)
					} else if n < ttlRet-1 {
						t.Fatal(n, ttlRet-1)
					}

					if n, err := bdb.ListObj.LLen(lkey, hash.Fnv32(lkey)); err != nil {
						t.Fatal(err)
					} else if n != listNum {
						t.Fatal(n)
					}
					if n, err := bdb.StringObj.TTL(lkey, hash.Fnv32(lkey)); err != nil {
						t.Fatal(err)
					} else if n < ttlRet-1 {
						t.Fatal(n, ttlRet-1)
					}

					if i == 8 {
						testCheckDelCmdExpireKey(t, bdb, stringKey, stkhash)
						testCheckDelCmdExpireKey(t, bdb, hkey, hkhash)
						testCheckDelCmdExpireKey(t, bdb, zkey, zkhash)
						testCheckDelCmdExpireKey(t, bdb, skey, skhash)
						testCheckDelCmdExpireKey(t, bdb, lkey, lkhash)
					}
					if i == 9 {
						testCheckExpireCmdExpireKey(t, bdb, stringKey, stkhash)
						testCheckExpireCmdExpireKey(t, bdb, hkey, hkhash)
						testCheckExpireCmdExpireKey(t, bdb, zkey, zkhash)
						testCheckExpireCmdExpireKey(t, bdb, skey, skhash)
						testCheckExpireCmdExpireKey(t, bdb, lkey, lkhash)
					}
				}
			}
		}()
	}
}

func TestKeys_FlushCheckExpire(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("TestKeys_CheckExpire_string_key")
		khash := hash.Fnv32(key)
		val := []byte("TestKeys_CheckExpire_string")
		if err := bdb.StringObj.Set(key, khash, val); err != nil {
			t.Fatal("Set err", string(key), err)
		}

		setkey := []byte("TestKeys_CheckExpire_set_key")
		setkhash := hash.Fnv32(setkey)
		setmember := []byte("TestKeys_CheckExpire_set_member")
		if n, err := bdb.SetObj.SAdd(setkey, setkhash, setmember); err != nil {
			t.Fatal("Set err", string(setkey), err)
		} else if n != 1 {
			t.Fatal("Set return n err", string(setkey), n)
		}

		hkey := []byte("TestKeys_CheckExpire_hash_key")
		hkhash := hash.Fnv32(hkey)
		hfield := []byte("TestKeys_CheckExpire_hash_field")
		hvalue := []byte("TestKeys_CheckExpire_hash_value")
		if n, err := bdb.HashObj.HSet(hkey, hkhash, hfield, hvalue); err != nil {
			t.Fatal("HSet err", string(hkey), err)
		} else if n != 1 {
			t.Fatal("HSet return n err", string(hkey), n)
		}

		zkey := []byte("TestKeys_CheckExpire_zset_key")
		zkhash := hash.Fnv32(zkey)
		zfield := []byte("TestKeys_CheckExpire_zset_field")
		if n, err := bdb.ZsetObj.ZAdd(zkey, zkhash, btools.ScorePair{Score: 1, Member: zfield}); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		lkey := []byte("TestKeys_CheckExpire_list_key")
		lkhash := hash.Fnv32(lkey)
		lfield := []byte("TestKeys_CheckExpire_list_lfield")
		lrfield := []byte("TestKeys_CheckExpire_list_lrfield")
		if n, err := bdb.ListObj.LPush(lkey, lkhash, lfield); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.ListObj.RPush(lkey, lkhash, lrfield); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		if n, err := bdb.StringObj.Expire(key, khash, 1); err != nil {
			t.Fatal("Expire err", string(key), err)
		} else if n != 1 {
			t.Fatal("Expire return n err", string(key), n)
		}
		if n, err := bdb.StringObj.Expire(setkey, setkhash, 1); err != nil {
			t.Fatal("Expire err", string(setkey), err)
		} else if n != 1 {
			t.Fatal("Expire return n err", string(setkey), n)
		}
		if n, err := bdb.StringObj.Expire(hkey, hkhash, 1); err != nil {
			t.Fatal("Expire err", string(hkey), err)
		} else if n != 1 {
			t.Fatal("Expire return n err", string(hkey), n)
		}
		if n, err := bdb.StringObj.Expire(zkey, zkhash, 1); err != nil {
			t.Fatal("Expire err", string(zkey), err)
		} else if n != 1 {
			t.Fatal("Expire return n err", string(zkey), n)
		}
		if n, err := bdb.StringObj.Expire(lkey, lkhash, 1); err != nil {
			t.Fatal("Expire err", string(lkey), err)
		} else if n != 1 {
			t.Fatal("Expire return n err", string(lkey), n)
		}

		testCheckKeyValue(t, bdb, key, khash, val)
		if n, err := bdb.SetObj.SCard(setkey, setkhash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.HashObj.HLen(hkey, hash.Fnv32(hkey)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.ZsetObj.ZCard(zkey, hash.Fnv32(zkey)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.ListObj.LLen(lkey, hash.Fnv32(lkey)); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		time.Sleep(2 * time.Second)
		bdb.FlushAllDB()
		bdb.ClearCache()

		ek, ekCloser := base.EncodeMetaKey(key, khash)
		strMk, strMkCloser, strMkErr := bdb.baseDb.GetMeta(ek)
		if strMkErr != nil || strMk != nil {
			t.Fatal("flush string key return err", strMk, strMkErr)
		}
		if strMkCloser != nil {
			strMkCloser()
		}
		ekCloser()

		ek, ekCloser = base.EncodeMetaKey(setkey, setkhash)
		setMk, setMkCloser, _ := bdb.baseDb.GetMeta(ek)
		if setMk == nil {
			t.Fatal("flush set key return not nil")
		}
		if setMkCloser != nil {
			setMkCloser()
		}
		ekCloser()

		ek, ekCloser = base.EncodeMetaKey(setkey, setkhash)
		hashMk, hashMkCloser, _ := bdb.baseDb.GetMeta(ek)
		if hashMk == nil {
			t.Fatal("flush hash key return not nil")
		}
		if hashMkCloser != nil {
			hashMkCloser()
		}
		ekCloser()

		ek, ekCloser = base.EncodeMetaKey(setkey, setkhash)
		zsetMk, zsetMkCloser, _ := bdb.baseDb.GetMeta(ek)
		if zsetMk == nil {
			t.Fatal("flush zset key return not nil")
		}
		if zsetMkCloser != nil {
			zsetMkCloser()
		}
		ekCloser()

		ek, ekCloser = base.EncodeMetaKey(setkey, setkhash)
		listMk, listMkCloser, _ := bdb.baseDb.GetMeta(ek)
		if listMk == nil {
			t.Fatal("flush list key return not nil")
		}
		if listMkCloser != nil {
			listMkCloser()
		}
		ekCloser()
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

		checkErrWrongType := func(dt btools.DataType) {
			if dt != btools.SET {
				if _, err := bdb.SetObj.SAdd(key, khash, sfield); err != errn.ErrWrongType {
					t.Fatal("SAdd ErrWrongType check fail", err)
				}
			}
			if dt != btools.ZSET {
				args := btools.ScorePair{Score: 1, Member: zfield}
				if _, err := bdb.ZsetObj.ZAdd(key, khash, args); err != errn.ErrWrongType {
					t.Fatal("ZAdd ErrWrongType check fail", err)
				}
			}
			if dt != btools.LIST {
				if _, err := bdb.ListObj.LPush(key, khash, llfield); err != errn.ErrWrongType {
					t.Fatal("LPush ErrWrongType check fail", err)
				}
				if _, err := bdb.ListObj.RPush(key, khash, lrfield); err != errn.ErrWrongType {
					t.Fatal("LPush ErrWrongType check fail", err)
				}
			}
			if dt != btools.HASH {
				if _, err := bdb.HashObj.HSet(key, khash, hfield, hvalue); err != errn.ErrWrongType {
					t.Fatal("HSet ErrWrongType check fail", err)
				}
				args := btools.FVPair{Field: hfield, Value: hvalue}
				if err := bdb.HashObj.HMset(key, khash, args); err != errn.ErrWrongType {
					t.Fatal("HMset ErrWrongType check fail", err)
				}
			}
		}

		if err := bdb.StringObj.Set(key, khash, key); err != nil {
			t.Fatal("Set err", err)
		}
		checkErrWrongType(btools.STRING)

		if n, err := bdb.StringObj.Del(khash, key); err != nil {
			t.Fatal("Del err", err)
		} else if n != 1 {
			t.Fatal("Del return n err", n)
		}

		if n, err := bdb.HashObj.HSet(key, khash, hfield, hvalue); err != nil {
			t.Fatal("HSet err", err)
		} else if n != 1 {
			t.Fatal("HSet return n err", n)
		}
		if n, err := bdb.HashObj.HLen(key, khash); err != nil {
			t.Fatal("HLen err", err)
		} else if n != 1 {
			t.Fatal("HLen return n err", n)
		}
		checkErrWrongType(btools.HASH)

		if n, err := bdb.StringObj.Del(khash, key); err != nil {
			t.Fatal("Del err", err)
		} else if n != 1 {
			t.Fatal("Del return n err", n)
		}

		if n, err := bdb.ListObj.LPush(key, khash, llfield); err != nil {
			t.Fatal("LPush err", err)
		} else if n != 1 {
			t.Fatal("LPush return n err", n)
		}
		if n, err := bdb.ListObj.RPush(key, khash, lrfield); err != nil {
			t.Fatal("RPush err", err)
		} else if n != 2 {
			t.Fatal("RPush return n err", n)
		}
		if n, err := bdb.ListObj.LLen(key, khash); err != nil {
			t.Fatal("LLen err", err)
		} else if n != 2 {
			t.Fatal("LLen return n err", n)
		}
		checkErrWrongType(btools.LIST)

		if n, err := bdb.StringObj.Expire(key, khash, 1); err != nil {
			t.Fatal("Expire err", err)
		} else if n != 1 {
			t.Fatal("Expire return n err", n)
		}
		time.Sleep(time.Second)

		if n, err := bdb.ZsetObj.ZAdd(key, khash, btools.ScorePair{Score: 1, Member: zfield}); err != nil {
			t.Fatal("Zadd err", err)
		} else if n != 1 {
			t.Fatal("Zadd return n err", n)
		}
		if n, err := bdb.ZsetObj.ZCard(key, khash); err != nil {
			t.Fatal("ZCard err", err)
		} else if n != 1 {
			t.Fatal("ZCard return n err", n)
		}
		checkErrWrongType(btools.ZSET)

		if n, err := bdb.StringObj.Expire(key, khash, 1); err != nil {
			t.Fatal("Expire err", err)
		} else if n != 1 {
			t.Fatal("Expire return n err", n)
		}
		time.Sleep(time.Second)

		if n, err := bdb.SetObj.SAdd(key, khash, sfield); err != nil {
			t.Fatal("SAdd err", err)
		} else if n != 1 {
			t.Fatal("SAdd return n err", n)
		}
		if n, err := bdb.SetObj.SCard(key, khash); err != nil {
			t.Fatal("SCard err", err)
		} else if n != 1 {
			t.Fatal("SCard return n err", n)
		}
		checkErrWrongType(btools.SET)
	}
}

func TestKeys_ScanBySlotId(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		var keys []string
		slotId := uint32(1)
		count := 10000
		sfield := []byte("TestKeys_set_field")
		hfield := []byte("TestKeys_hash_field")
		hvalue := []byte("TestKeys_hash_value")
		llfield := []byte("TestKeys_list_llfield")
		zfield := []byte("TestKeys_zset_field")

		index := 0
		for {
			k := fmt.Sprintf("TestScanBySlotIdKey_%d", index)
			index++

			if uint32(utils.GetSlotId(hash.Fnv32([]byte(k)))) != slotId {
				continue
			}

			keys = append(keys, k)
			if len(keys) == count {
				break
			}
		}

		sort.Strings(keys)

		for i := 0; i < count; i++ {
			key := []byte(keys[i])
			khash := hash.Fnv32(key)
			switch i % 5 {
			case 0:
				if err := bdb.StringObj.Set(key, khash, key); err != nil {
					t.Fatal("Set err", err)
				}
			case 1:
				if n, err := bdb.SetObj.SAdd(key, khash, sfield); err != nil {
					t.Fatal("SAdd err", err)
				} else if n != 1 {
					t.Fatal("SAdd return n err", n)
				}
			case 2:
				if n, err := bdb.ZsetObj.ZAdd(key, khash, btools.ScorePair{1, zfield}); err != nil {
					t.Fatal("Zadd err", err)
				} else if n != 1 {
					t.Fatal("Zadd return n err", n)
				}
			case 3:
				if n, err := bdb.HashObj.HSet(key, khash, hfield, hvalue); err != nil {
					t.Fatal("HSet err", err)
				} else if n != 1 {
					t.Fatal("HSet return n err", n)
				}
			case 4:
				if n, err := bdb.ListObj.LPush(key, khash, llfield); err != nil {
					t.Fatal("LPush err", err)
				} else if n != 1 {
					t.Fatal("LPush return n err", n)
				}
			}
		}

		limit := 1000
		cnt := 0
		var err error
		var cursor []byte
		var scanList []btools.ScanPair
		var expDt btools.DataType

		for {
			cursor, scanList, err = bdb.ScanBySlotId(slotId, cursor, limit, "*")
			require.NoError(t, err)

			for i := range scanList {
				scanKey := []byte(keys[cnt])
				require.Equal(t, scanKey, scanList[i].Key)

				dt := scanList[i].Dt
				switch cnt % 5 {
				case 0:
					expDt = btools.STRING
				case 1:
					expDt = btools.SET
				case 2:
					expDt = btools.ZSET
				case 3:
					expDt = btools.HASH
				case 4:
					expDt = btools.LIST
				}
				require.Equal(t, expDt, dt)
				cnt++
			}

			if len(scanList) < limit || bytes.Equal(cursor, btools.ScanEndCurosr) {
				break
			}
		}

		require.Equal(t, count, cnt)
	}
}
