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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
)

func TestSetCodec(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("key")
		member := []byte("member")
		keyId := bdb.SetObj.GetNextKeyId()

		khash := hash.Fnv32(key)
		ek, ekCloser := base.EncodeMetaKey(key, khash)
		if k, err := base.DecodeMetaKey(ek); err != nil {
			t.Fatal(err)
		} else if string(k) != "key" {
			t.Fatal(string(k))
		}
		ekCloser()
		mkv := &base.MetaData{}
		mkv.SetDataType(btools.SET)
		mkv.Reset(keyId)
		keyKind := base.KeyKindFieldCompress
		efk, efkCloser, isCompress := base.EncodeSetDataKey(keyId, keyKind, khash, member)
		require.Equal(t, false, isCompress)
		decVersion, fp := base.DecodeSetDataKey(keyKind, efk, []byte{base.DataValueKindDefault})
		decMember := fp.Merge()
		if decVersion != keyId {
			t.Fatal("version err", decVersion, keyId)
		} else if !bytes.Equal(member, decMember) {
			t.Fatal("member err", decMember)
		}
		efkCloser()

		member = testRandBytes(base.KeyFieldCompressSize * 2)
		keyId = bdb.SetObj.GetNextKeyId()
		mkv.Reset(keyId)
		efk, efkCloser, isCompress = base.EncodeSetDataKey(keyId, keyKind, khash, member)
		require.Equal(t, true, isCompress)
		value := make([]byte, len(member)-base.KeyFieldCompressPrefix+1)
		value[0] = base.DataValueKindFieldCompress
		copy(value[1:], member[base.KeyFieldCompressPrefix:])
		decVersion, fp = base.DecodeSetDataKey(keyKind, efk, value)
		decMember = fp.Merge()
		if decVersion != keyId {
			t.Fatal("version err", decVersion, keyId)
		} else if !bytes.Equal(member, decMember) {
			t.Fatal("member err", string(decMember))
		}
		efkCloser()
	}
}

func TestDBSRandMemberAndSPop(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("TestDBSRandMemberAndSPop_key")
		khash := hash.Fnv32(key)
		member1 := testRandBytes(base.KeyFieldCompressSize / 2)
		member2 := testRandBytes(base.KeyFieldCompressSize * 2)

		if n, err := bdb.SetObj.SAdd(key, khash, member1, member2); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		if cnt, err := bdb.SetObj.SCard(key, khash); err != nil {
			t.Fatal(err)
		} else if cnt != 2 {
			t.Fatal(cnt)
		}

		if n, err := bdb.SetObj.SAdd(key, khash, member1); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if n, err := bdb.SetObj.SRem(key, khash, member1); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if cnt, err := bdb.SetObj.SCard(key, khash); err != nil {
			t.Fatal(err)
		} else if cnt != 1 {
			t.Fatal(cnt)
		}

		if n, err := bdb.SetObj.SAdd(key, khash, member1); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if cnt, err := bdb.SetObj.SCard(key, khash); err != nil {
			t.Fatal(err)
		} else if cnt != 2 {
			t.Fatal(cnt)
		}

		checkMember := func(m []byte) {
			if len(m) > base.KeyFieldCompressSize {
				if !bytes.Equal(m, member2) {
					t.Fatal("SRandMember compress m != member2")
				}
			} else {
				if !bytes.Equal(m, member1) {
					t.Fatal("SRandMember nocompress m != member1")
				}
			}
		}

		if m, err := bdb.SetObj.SRandMember(key, khash, 1); err != nil {
			t.Fatal(err)
		} else if len(m) != 1 {
			t.Fatal("SRandMember 1 len err")
		} else {
			checkMember(m[0])
		}

		if members, err := bdb.SetObj.SRandMember(key, khash, 2); err != nil {
			t.Fatal(err)
		} else if len(members) != 2 {
			t.Fatal("SRandMember 2 len err")
		} else {
			checkMember(members[0])
			checkMember(members[1])
		}

		if members, err := bdb.SetObj.SRandMember(key, khash, -2); err != nil {
			t.Fatal(err)
		} else if len(members) != 2 {
			t.Fatal(len(members))
		} else {
			checkMember(members[0])
			checkMember(members[1])
		}

		if m, err := bdb.SetObj.SPop(key, khash, 1); err != nil {
			t.Fatal(err)
		} else if len(m) != 1 {
			t.Fatal("Spop len err")
		} else {
			checkMember(m[0])
		}

		if m, err := bdb.SetObj.SPop(key, khash, 1); err != nil {
			t.Fatal(err)
		} else if len(m) != 1 {
			t.Fatal("Spop len err")
		} else {
			checkMember(m[0])
		}

		if cnt, err := bdb.SetObj.SCard(key, khash); err != nil {
			t.Fatal(err)
		} else if cnt != 0 {
			t.Fatal(cnt)
		}
	}
}

func TestDBSRandMember(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("TestDBSRandMember_key")
		khash := hash.Fnv32(key)

		for i := 0; i < 10; i++ {
			if n, err := bdb.SetObj.SAdd(key, khash, []byte(fmt.Sprintf("member%d", i))); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}
		}

		if cnt, err := bdb.SetObj.SCard(key, khash); err != nil {
			t.Fatal(err)
		} else if cnt != 10 {
			t.Fatal(cnt)
		}

		if members, err := bdb.SetObj.SRandMember(key, khash, 1); err != nil {
			t.Fatal(err)
		} else if len(members) != 1 {
			t.Fatal(len(members))
		}

		if members, err := bdb.SetObj.SRandMember(key, khash, -4); err != nil {
			t.Fatal(err)
		} else if len(members) != 4 {
			t.Fatal(len(members))
		}

		if members, err := bdb.SetObj.SRandMember(key, khash, 100); err != nil {
			t.Fatal(err)
		} else if len(members) != 10 {
			t.Fatal(len(members))
		}

		if members, err := bdb.SetObj.SRandMember(key, khash, -20); err != nil {
			t.Fatal(err)
		} else if len(members) != 20 {
			t.Fatal(len(members))
		}

		if cnt, err := bdb.SetObj.SCard(key, khash); err != nil {
			t.Fatal(err)
		} else if cnt != 10 {
			t.Fatal(cnt)
		}
	}
}

func TestDBSet(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("testdb_set_a")
		khash := hash.Fnv32(key)
		member := []byte("member")
		key1 := []byte("testdb_set_a1")
		k1hash := hash.Fnv32(key1)
		key2 := []byte("testdb_set_a2")
		k2hash := hash.Fnv32(key2)
		member1 := testRandBytes(base.KeyFieldCompressSize)
		member2 := testRandBytes(base.KeyFieldCompressSize * 10)

		defer bdb.SetObj.Del(khash, key, key1, key2)

		if n, err := bdb.StringObj.Exists(key, khash); err != nil {
			t.Fatal(err.Error())
		} else if n != 0 {
			t.Fatal("invalid value ", n)
		}

		if n, err := bdb.SetObj.SAdd(key, khash, member); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		mk, mkCloser := base.EncodeMetaKey(key, khash)
		mkv, err := bdb.SetObj.GetMetaData(mk)
		mkCloser()
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, base.KeyKindFieldCompress, mkv.Kind())

		if n, err := bdb.StringObj.Exists(key, khash); err != nil {
			t.Fatal(err.Error())
		} else if n != 1 {
			t.Fatal("invalid value ", n)
		}

		if cnt, err := bdb.SetObj.SCard(key, khash); err != nil {
			t.Fatal(err)
		} else if cnt != 1 {
			t.Fatal(cnt)
		}

		if n, err := bdb.SetObj.SIsMember(key, khash, member); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		v, err := bdb.SetObj.SMembers(key, khash)
		if err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(v[0], member) {
			t.Fatal("member err")
		}

		if n, err := bdb.SetObj.SRem(key, khash, member); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		bdb.SetObj.SAdd(key1, k1hash, member1, member2)

		if n, err := bdb.SetObj.Del(k1hash, key1); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		bdb.SetObj.SAdd(key1, k1hash, member1, member2)
		bdb.SetObj.SAdd(key2, k2hash, member1, member2, []byte("xxx"))

		if n, _ := bdb.SetObj.SCard(key1, k1hash); n != 2 {
			t.Fatal(n)
		}

		if n, _ := bdb.SetObj.SCard(key2, k2hash); n != 3 {
			t.Fatal(n)
		}
		if n, err := bdb.SetObj.Del(k1hash, key1, key2); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		bdb.SetObj.SAdd(key2, k2hash, member1, member2)
		if n, err := bdb.StringObj.Expire(key2, k2hash, 3600); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := bdb.StringObj.ExpireAt(key2, k2hash, time.Now().Unix()+3600); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := bdb.StringObj.TTL(key2, k2hash); err != nil {
			t.Fatal(err)
		} else if n < 0 {
			t.Fatal(n)
		}

		if n, err := bdb.StringObj.BasePersist(key2, k2hash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
	}
}

func TestDBSetDuplicate(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("test_set_duplicate")
		khash := hash.Fnv32(key)
		member0 := []byte("testdb_set_m0")
		member1 := testRandBytes(base.KeyFieldCompressSize)
		member2 := testRandBytes(base.KeyFieldCompressSize * 3)

		defer bdb.SetObj.Del(khash, key)

		if n, err := bdb.SetObj.SAdd(key, khash, member0, member1, member2); err != nil {
			t.Fatal(err)
		} else if n != 3 {
			t.Fatal(n)
		}

		if cnt, err := bdb.SetObj.SCard(key, khash); err != nil {
			t.Fatal(err)
		} else if cnt != 3 {
			t.Fatal(cnt)
		}

		if n, err := bdb.SetObj.SAdd(key, khash, member0, member1, member2); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if cnt, err := bdb.SetObj.SCard(key, khash); err != nil {
			t.Fatal(err)
		} else if cnt != 3 {
			t.Fatal(cnt)
		}
	}
}

func TestDBSScan(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("sscan_s_key")
		khash := hash.Fnv32(key)
		defer bdb.SetObj.Del(khash, key)

		member1 := []byte("1")
		member2 := []byte(fmt.Sprintf("2%s", string(testRandBytes(base.KeyFieldCompressSize-1))))
		member3 := []byte(fmt.Sprintf("3%s", string(testRandBytes(base.KeyFieldCompressSize))))
		member4 := []byte(fmt.Sprintf("42%s", string(testRandBytes(base.KeyFieldCompressSize*5))))

		n, err := bdb.SetObj.SAdd(key, khash, member1, member2, member3, member4)
		if err != nil {
			t.Fatal(err)
		} else if n != 4 {
			t.Fatal("SAdd fail n=", n)
		}

		cursor, v, err := bdb.SetObj.SScan(key, khash, nil, 100, "**")
		if err != nil {
			t.Fatal(err)
		} else if len(v) != 4 {
			t.Fatal("invalid count", len(v))
		} else if string(cursor) != "0" {
			t.Fatal("cursor not empty")
		} else {
			if !bytes.Equal(v[0], member1) {
				t.Fatal("member1 err")
			}
			if !bytes.Equal(v[1], member2) {
				t.Fatal("member2 err")
			}
			if !bytes.Equal(v[2], member3) {
				t.Fatal("member3 err")
			}
			if !bytes.Equal(v[3], member4) {
				t.Fatal("member4 err")
			}
		}

		cursor, v, err = bdb.SetObj.SScan(key, khash, nil, 1, "**")
		if err != nil {
			t.Fatal(err)
		} else if len(v) != 1 {
			t.Fatal("invalid count", len(v))
		} else if !bytes.Equal(cursor, member2) {
			t.Fatal("cursor err")
		} else if !bytes.Equal(v[0], member1) {
			t.Fatal("member1 err")
		}

		cur4 := append([]byte{}, member4[:base.KeyFieldCompressPrefix]...)
		m4Md5 := md5.Sum(member4)
		cur4 = append(cur4, m4Md5[:]...)
		cursor, v, err = bdb.SetObj.SScan(key, khash, nil, 3, "**")
		if err != nil {
			t.Fatal(err)
		} else if len(v) != 3 {
			t.Fatal("invalid count", len(v))
		} else if !bytes.Equal(cursor, cur4) {
			t.Fatal("cur4 err")
		} else {
			if !bytes.Equal(v[0], member1) {
				t.Fatal("member1 err")
			}
			if !bytes.Equal(v[1], member2) {
				t.Fatal("member2 err")
			}
			if !bytes.Equal(v[2], member3) {
				t.Fatal("member3 err")
			}
		}
		cursor, v, err = bdb.SetObj.SScan(key, khash, cursor, 2, "**")
		if err != nil {
			t.Fatal(err)
		} else if len(v) != 1 {
			t.Fatal("invalid count", len(v))
		} else if string(cursor) != "0" {
			t.Fatal("cursor not empty")
		} else if !bytes.Equal(v[0], member4) {
			t.Fatal("member4 err")
		}

		cursor, v, err = bdb.SetObj.SScan(key, khash, []byte("3"), 1, "**")
		if err != nil {
			t.Fatal(err)
		} else if len(v) != 1 {
			t.Fatal("invalid count", len(v))
		} else if !bytes.Equal(v[0], member3) {
			t.Fatal("SScan cursor 3 err")
		}
	}
}

func TestSetDBScan(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key1 := []byte("scan_aaa")
		k1hash := hash.Fnv32(key1)
		key2 := []byte("scan_bbb")
		k2hash := hash.Fnv32(key2)

		bdb.SetObj.SAdd(key1, k1hash, []byte("1"), []byte("222"), []byte("19"), []byte("1234"))
		bdb.SetObj.SAdd(key2, k2hash, []byte("a"), []byte("b"), []byte("c"), []byte("d"))

		cursor, v, err := bdb.Scan(nil, 100, "", btools.SET)
		if err != nil {
			t.Fatal(err)
		} else if len(v) != 2 {
			t.Fatal("invalid count", len(v))
		} else if string(cursor) != "0" {
			t.Fatal("cursor not 0")
		}

		cursor, v, err = bdb.Scan([]byte("scan_aaa"), 1, "", btools.SET)
		if err != nil {
			t.Fatal(err)
		} else if len(v) != 1 {
			t.Fatal("invalid count", len(v))
		} else if string(v[0]) != "scan_aaa" {
			t.Fatal(string(v[0]))
		} else if string(cursor) != "scan_bbb" {
			t.Fatal(string(cursor))
		}
	}
}

func TestDBSetKeyKind(t *testing.T) {
	cores := testTwoBitsCores()
	defer closeCores(cores)

	for _, cr := range cores {
		bdb := cr.db

		key := []byte("testdb_set_a")
		khash := hash.Fnv32(key)
		key1 := []byte("testdb_set_a1")
		k1hash := hash.Fnv32(key1)
		member1 := []byte(fmt.Sprintf("1%s", string(testRandBytes(base.KeyFieldCompressSize-10))))
		member2 := []byte(fmt.Sprintf("2%s", string(testRandBytes(base.KeyFieldCompressSize-1))))
		member3 := []byte(fmt.Sprintf("3%s", string(testRandBytes(base.KeyFieldCompressSize*10))))

		checkKeyKind := func(k []byte, h uint32, kind uint8) {
			mk, mkCloser := base.EncodeMetaKey(k, h)
			mkv, err := bdb.SetObj.GetMetaData(mk)
			mkCloser()
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, kind, mkv.Kind())
		}

		checkCmd := func(key []byte, khash uint32, kind uint8) {
			checkKeyKind(key, khash, kind)

			if cnt, err := bdb.SetObj.SCard(key, khash); err != nil {
				t.Fatal(err)
			} else if cnt != 3 {
				t.Fatal(cnt)
			}
			if n, err := bdb.SetObj.SIsMember(key, khash, member2); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}
			if n, err := bdb.SetObj.SIsMember(key, khash, []byte("123")); err != nil {
				t.Fatal(err)
			} else if n != 0 {
				t.Fatal(n)
			}
			if v, err := bdb.SetObj.SMembers(key, khash); err != nil {
				t.Fatal(err)
			} else if len(v) != 3 {
				t.Fatal("SMembers len err", len(v))
			} else if !bytes.Equal(v[0], member1) {
				t.Fatal("SMembers member1 err")
			} else if !bytes.Equal(v[1], member2) {
				t.Fatal("SMembers member2 err")
			} else if !bytes.Equal(v[2], member3) {
				t.Fatal("SMembers member3 err")
			}
			if n, err := bdb.SetObj.SRem(key, khash, member1); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}
			if cnt, err := bdb.SetObj.SCard(key, khash); err != nil {
				t.Fatal(err)
			} else if cnt != 2 {
				t.Fatal(cnt)
			}
			if n, err := bdb.StringObj.Expire(key, khash, 3600); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}
			if n, err := bdb.StringObj.ExpireAt(key, khash, time.Now().Unix()+3600); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}
			if n, err := bdb.StringObj.TTL(key, khash); err != nil {
				t.Fatal(err)
			} else if n < 0 {
				t.Fatal(n)
			}
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
			checkKeyKind(key, khash, kind)
		}

		if n, err := bdb.SetObj.SAdd(key, khash, member1); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.SetObj.SAdd(key, khash, member2, member3); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}
		checkCmd(key, khash, base.KeyKindFieldCompress)

		if n, err := bdb.SetObj.SAdd(key1, k1hash, member1, member2, member3); err != nil {
			t.Fatal(err)
		} else if n != 3 {
			t.Fatal(n)
		}
		checkCmd(key1, k1hash, base.KeyKindFieldCompress)
	}
}
