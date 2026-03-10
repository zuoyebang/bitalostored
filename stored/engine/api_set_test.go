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
	"sort"
	"testing"
	"time"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb"

	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/stretchr/testify/require"
)

func getSeparationSortedMembers(num int) [][]byte {
	memebers := make([][]byte, num)
	for i := 0; i < num; i++ {
		memebers[i] = testRandBytes(100)
	}

	sort.Slice(memebers, func(i, j int) bool {
		return bytes.Compare(memebers[i], memebers[j]) < 0
	})

	sort.Slice(memebers, func(i, j int) bool {
		return bytes.Compare(memebers[i], memebers[j]) < 0
	})

	return memebers
}

func TestDBSetCMD(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	num := 100
	memebers := getSeparationSortedMembers(num)
	for i := 0; i < 10; i++ {
		key := testMakeIntKey(i)
		khash := hash.Fnv32(key)
		step := 5
		for j := 0; j < num; j += step {
			mems := memebers[j : j+step]
			n, err := bdb.db.SAdd(key, khash, mems...)
			require.NoError(t, err)
			require.Equal(t, int64(step), n)
		}

		n, err := bdb.db.SCard(key, khash)
		require.NoError(t, err)
		require.Equal(t, int64(num), n)
	}

	popNum := 10
	for i := 0; i < 10; i++ {
		key := testMakeIntKey(i)
		khash := hash.Fnv32(key)
		mbs, closer, err := bdb.db.SPop(key, khash, int64(popNum))
		require.NoError(t, err)
		require.Equal(t, popNum, len(mbs))
		for j := 0; j < popNum; j++ {
			require.Equal(t, memebers[j], mbs[j])
			n, e := bdb.db.SIsMember(key, khash, mbs[j])
			require.NoError(t, e)
			require.Equal(t, int64(0), n)
		}
		closer()

		n, e := bdb.db.SCard(key, khash)
		require.NoError(t, e)
		require.Equal(t, int64(num-popNum), n)
	}

	require.NoError(t, bdb.db.bitsdb.DB.Flush(false))

	popNum = 10
	pos := 2 * popNum
	leftNum := num - pos
	for i := 0; i < 10; i++ {
		key := testMakeIntKey(i)
		khash := hash.Fnv32(key)
		mbs, closer, err := bdb.db.SPop(key, khash, int64(popNum))
		require.NoError(t, err)
		require.Equal(t, popNum, len(mbs))
		for j := 0; j < popNum; j++ {
			require.Equal(t, memebers[popNum+j], mbs[j])
			n, e := bdb.db.SIsMember(key, khash, mbs[j])
			require.NoError(t, e)
			require.Equal(t, int64(0), n)
		}
		closer()

		n, e := bdb.db.SCard(key, khash)
		require.NoError(t, e)
		require.Equal(t, int64(leftNum), n)
	}

	for i := 0; i < 10; i++ {
		key := testMakeIntKey(i)
		khash := hash.Fnv32(key)
		mbs, closer, err := bdb.db.SMembers(key, khash)
		require.NoError(t, err)
		require.Equal(t, leftNum, len(mbs))
		for j := 0; j < popNum; j++ {
			require.Equal(t, memebers[pos+j], mbs[j])
		}
		closer()
	}
}

func TestDBSRandMemberAndSPop(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	key := []byte("TestDBSRandMemberAndSPop_key")
	khash := hash.Fnv32(key)
	member1 := testRandBytes(100)
	member2 := testRandBytes(100)

	if n, err := bdb.db.SAdd(key, khash, member1, member2); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if cnt, err := bdb.db.SCard(key, khash); err != nil {
		t.Fatal(err)
	} else if cnt != 2 {
		t.Fatal(cnt)
	}

	if n, err := bdb.db.SAdd(key, khash, member1); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := bdb.db.SRem(key, khash, member1); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if cnt, err := bdb.db.SCard(key, khash); err != nil {
		t.Fatal(err)
	} else if cnt != 1 {
		t.Fatal(cnt)
	}

	if n, err := bdb.db.SAdd(key, khash, member1); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if cnt, err := bdb.db.SCard(key, khash); err != nil {
		t.Fatal(err)
	} else if cnt != 2 {
		t.Fatal(cnt)
	}

	checkMember := func(m []byte) {
		if !bytes.Equal(m, member1) && !bytes.Equal(m, member2) {
			t.Fatal("SRandMember nocompress m != member")
		}
	}

	if m, closer, err := bdb.db.SRandMember(key, khash, 1); err != nil {
		t.Fatal(err)
	} else if len(m) != 1 {
		t.Fatal("SRandMember 1 len err")
	} else {
		checkMember(m[0])
		closer()
	}

	if m, closer, err := bdb.db.SRandMember(key, khash, 2); err != nil {
		t.Fatal(err)
	} else if len(m) != 2 {
		t.Fatal("SRandMember 2 len err")
	} else {
		checkMember(m[0])
		checkMember(m[1])
		closer()
	}

	if m, closer, err := bdb.db.SRandMember(key, khash, -2); err != nil {
		t.Fatal(err)
	} else if len(m) != 2 {
		t.Fatal(len(m))
	} else {
		checkMember(m[0])
		checkMember(m[1])
		closer()
	}

	if m, closer, err := bdb.db.SPop(key, khash, 1); err != nil {
		t.Fatal(err)
	} else if len(m) != 1 {
		t.Fatal("Spop len err")
	} else {
		checkMember(m[0])
		closer()
	}

	if m, closer, err := bdb.db.SPop(key, khash, 1); err != nil {
		t.Fatal(err)
	} else if len(m) != 1 {
		t.Fatal("Spop len err")
	} else {
		checkMember(m[0])
		closer()
	}

	if cnt, err := bdb.db.SCard(key, khash); err != nil {
		t.Fatal(err)
	} else if cnt != 0 {
		t.Fatal(cnt)
	}
}

func TestDBSRandMember(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	key := []byte("TestDBSRandMember_key")
	khash := hash.Fnv32(key)

	for i := 0; i < 10; i++ {
		if n, err := bdb.db.SAdd(key, khash, []byte(fmt.Sprintf("member%d", i))); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
	}

	if cnt, err := bdb.db.SCard(key, khash); err != nil {
		t.Fatal(err)
	} else if cnt != 10 {
		t.Fatal(cnt)
	}

	members, closer, err := bdb.db.SRandMember(key, khash, 1)
	require.NoError(t, err)
	require.Equal(t, 1, len(members))
	closer()

	members, closer, err = bdb.db.SRandMember(key, khash, -4)
	require.NoError(t, err)
	require.Equal(t, 4, len(members))
	closer()

	members, closer, err = bdb.db.SRandMember(key, khash, 100)
	require.NoError(t, err)
	require.Equal(t, 10, len(members))
	closer()

	members, closer, err = bdb.db.SRandMember(key, khash, -20)
	require.NoError(t, err)
	require.Equal(t, 20, len(members))
	closer()

	if cnt, err := bdb.db.SCard(key, khash); err != nil {
		t.Fatal(err)
	} else if cnt != 10 {
		t.Fatal(cnt)
	}
}

func TestDBSet(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	key := []byte("testdb_set_a")
	khash := hash.Fnv32(key)
	member := []byte("member")
	key1 := []byte("testdb_set_a1")
	k1hash := hash.Fnv32(key1)
	key2 := []byte("testdb_set_a2")
	k2hash := hash.Fnv32(key2)
	member1 := testRandBytes(100)
	member2 := testRandBytes(200)

	n, err := bdb.db.Exists(key, khash)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	n, err = bdb.db.SAdd(key, khash, member)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, err = bdb.db.Exists(key, khash)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, err = bdb.db.SCard(key, khash)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, err = bdb.db.SIsMember(key, khash, member)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	v, closer, err1 := bdb.db.SMembers(key, khash)
	require.NoError(t, err1)
	require.Equal(t, member, v[0])
	closer()

	n, err = bdb.db.SRem(key, khash, member)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, err = bdb.db.SAdd(key1, k1hash, member1, member2)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	n, err = bdb.db.SRem(key1, k1hash, member2)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	n, err = bdb.db.SIsMember(key1, k1hash, member1)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	n, err = bdb.db.SIsMember(key1, k1hash, member2)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	n, err = bdb.db.SAdd(key1, k1hash, member1, member2)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, err = bdb.db.SAdd(key2, k2hash, member1, member2, []byte("xxx"))
	require.NoError(t, err)
	require.Equal(t, int64(3), n)

	n, err = bdb.db.SCard(key1, k1hash)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	n, err = bdb.db.SCard(key2, k2hash)
	require.NoError(t, err)
	require.Equal(t, int64(3), n)

	n, err = bdb.db.Del(k1hash, key1, key2)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	n, err = bdb.db.SAdd(key2, k2hash, member1, member2)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	n, err = bdb.db.Expire(key2, k2hash, 3600)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, err = bdb.db.ExpireAt(key2, k2hash, time.Now().Unix()+3600)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	if n, err = bdb.db.TTL(key2, k2hash); err != nil {
		t.Fatal(err)
	} else if n < 0 {
		t.Fatal(n)
	}

	n, err = bdb.db.Persist(key2, k2hash)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, err = bdb.db.Del(khash, key, key1, key2)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
}

func TestDBSetDuplicate(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	key := []byte("test_set_duplicate")
	khash := hash.Fnv32(key)
	member0 := []byte("testdb_set_m0")
	member1 := testRandBytes(100)
	member2 := testRandBytes(100)

	if n, err := bdb.db.SAdd(key, khash, member0, member1, member2); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if cnt, err := bdb.db.SCard(key, khash); err != nil {
		t.Fatal(err)
	} else if cnt != 3 {
		t.Fatal(cnt)
	}

	if n, err := bdb.db.SAdd(key, khash, member0, member1, member2); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if cnt, err := bdb.db.SCard(key, khash); err != nil {
		t.Fatal(err)
	} else if cnt != 3 {
		t.Fatal(cnt)
	}

	bdb.db.Del(khash, key)
}

func TestDBSetKeyKind(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	key := []byte("testdb_set_a")
	khash := hash.Fnv32(key)
	key1 := []byte("testdb_set_a1")
	k1hash := hash.Fnv32(key1)
	member1 := []byte(fmt.Sprintf("1%s", string(testRandBytes(10))))
	member2 := []byte(fmt.Sprintf("2%s", string(testRandBytes(10))))
	member3 := []byte(fmt.Sprintf("3%s", string(testRandBytes(10))))
	memberMap := make(map[string]bool)
	memberMap[string(member1)] = true
	memberMap[string(member2)] = true
	memberMap[string(member3)] = true

	checkCmd := func(key []byte, khash uint32) {
		if cnt, err := bdb.db.SCard(key, khash); err != nil {
			t.Fatal(err)
		} else if cnt != 3 {
			t.Fatal(cnt)
		}
		if n, err := bdb.db.SIsMember(key, khash, member2); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.db.SIsMember(key, khash, []byte("123")); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		members, closer, err := bdb.db.SMembers(key, khash)
		require.NoError(t, err)
		require.Equal(t, 3, len(members))
		require.Equal(t, true, memberMap[string(members[0])])
		require.Equal(t, true, memberMap[string(members[1])])
		require.Equal(t, true, memberMap[string(members[2])])
		closer()

		if n, err := bdb.db.SRem(key, khash, member1); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if cnt, err := bdb.db.SCard(key, khash); err != nil {
			t.Fatal(err)
		} else if cnt != 2 {
			t.Fatal(cnt)
		}
		if n, err := bdb.db.Expire(key, khash, 3600); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.db.ExpireAt(key, khash, time.Now().Unix()+3000); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.db.TTL(key, khash); err != nil {
			t.Fatal(err)
		} else if n < 0 {
			t.Fatal(n)
		}
		if n, err := bdb.db.Persist(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.db.TTL(key, khash); err != nil {
			t.Fatal(err)
		} else if n != bitsdb.ErrnoKeyPersist {
			t.Fatal(n)
		}
	}

	if n, err := bdb.db.SAdd(key, khash, member1); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	if n, err := bdb.db.SAdd(key, khash, member2, member3); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}
	checkCmd(key, khash)

	if n, err := bdb.db.SAdd(key1, k1hash, member1, member2, member3); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}
	checkCmd(key1, k1hash)
}
