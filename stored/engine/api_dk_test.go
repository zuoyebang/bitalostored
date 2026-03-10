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
	"testing"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb"
	"github.com/zuoyebang/bitalostored/stored/engine/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"

	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalosdb/v2"
	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/hash"
)

func TestDBDKCMD(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	var dt uint8
	ts := tclock.GetTimestampMilli() + 10*1000
	for i := 0; i < 10; i++ {
		key := testMakeIntKey(i)
		khash := hash.Fnv32(key)
		shardNum := uint32(i + 1)
		if i%2 == 0 {
			dt = btools.DataTypeHash
		} else {
			dt = btools.DataTypeSet
		}
		require.NoError(t, bdb.db.DKCreate(key, khash, dt, shardNum))

		n, err := bdb.db.DKIncrBySize(key, khash, 10)
		require.NoError(t, err)
		require.Equal(t, int64(10), n)

		sz, err1 := bdb.db.DKIncrBySize(key, khash, -2)
		require.NoError(t, err1)
		require.Equal(t, int64(8), sz)

		dataType, sn, size, ttl := bdb.db.DKInfo(key, khash)
		require.Equal(t, dt, dataType)
		require.Equal(t, shardNum, sn)
		require.Equal(t, uint64(sz), size)
		require.Equal(t, int64(bitsdb.ErrnoKeyPersist), ttl)

		n, err = bdb.db.PExpireAt(key, khash, ts)
		require.NoError(t, err)
		require.Equal(t, int64(1), n)

		dataType, sn, size, ttl = bdb.db.DKInfo(key, khash)
		require.Equal(t, dt, dataType)
		require.Equal(t, shardNum, sn)
		require.Equal(t, uint64(sz), size)
		require.Equal(t, int64(10), ttl)

		if i%3 == 0 {
			n, err = bdb.db.Del(khash, key)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
		}

		dataType, sn, size, ttl = bdb.db.DKInfo(key, khash)
		if i%3 == 0 {
			require.Equal(t, uint8(0), dataType)
			require.Equal(t, uint32(0), sn)
			require.Equal(t, uint64(0), size)
			require.Equal(t, int64(bitsdb.ErrnoKeyNotFoundOrExpire), ttl)
			require.NoError(t, bdb.db.DKCreate(key, khash, dt, shardNum))
			bdb.db.DKIncrBySize(key, khash, 10)
			dataType, sn, size, ttl = bdb.db.DKInfo(key, khash)
			require.Equal(t, dt, dataType)
			require.Equal(t, shardNum, sn)
			require.Equal(t, uint64(10), size)
			require.Equal(t, int64(bitsdb.ErrnoKeyPersist), ttl)
		} else {
			require.Equal(t, dt, dataType)
			require.Equal(t, shardNum, sn)
			require.Equal(t, uint64(sz), size)
			if ttl < 1 {
				t.Fatal("ttl should be larger than 1")
			}
		}
	}

	key := testMakeIntKey(100)
	khash := hash.Fnv32(key)
	dataType, sn, size, ttl := bdb.db.DKInfo(key, khash)
	require.Equal(t, uint8(0), dataType)
	require.Equal(t, uint32(0), sn)
	require.Equal(t, uint64(0), size)
	require.Equal(t, int64(bitsdb.ErrnoKeyNotFoundOrExpire), ttl)
}

func TestDKHash(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	var wargs, rargs, rres, dargs, dres [][]byte
	kn := 10
	kkn := 10
	kknBytes := extend.FormatIntToSlice(kkn)
	delBytes := extend.FormatIntToSlice(kkn / 2)
	for i := 0; i < kn; i++ {
		key := []byte(fmt.Sprintf("dkhash_%d", i))
		wargs = append(wargs, key, kknBytes)
		rargs = append(rargs, key, kknBytes)
		dargs = append(dargs, key, delBytes)
		rres = append(rres, key, kknBytes)
		dres = append(dres, key, kknBytes)
		for j := 0; j < kkn; j++ {
			f := []byte(fmt.Sprintf("dkhashfield_%d", j))
			v := []byte(fmt.Sprintf("dkhashvalue_%d", j))
			wargs = append(wargs, f, v)
			rargs = append(rargs, f)
			rres = append(rres, v)
			if j%2 == 0 {
				dargs = append(dargs, f)
				dres = append(dres, []byte(nil))
			} else {
				dres = append(dres, v)
			}
		}
	}

	resSet, errSet := bdb.db.DKHSet(wargs...)
	require.Equal(t, bitalosdb.ErrNotFound, errSet)

	for i := 0; i < kn; i++ {
		key := []byte(fmt.Sprintf("dkhash_%d", i))
		err := bdb.db.DKCreateShard(btools.DataTypeHash, key)
		require.NoError(t, err)
	}

	resSet, errSet = bdb.db.DKHSet(wargs...)
	require.NoError(t, errSet)
	require.Equal(t, kn*2, len(resSet))
	for i := 0; i < kn; i++ {
		key := []byte(fmt.Sprintf("dkhash_%d", i))
		require.Equal(t, key, resSet[i*2])
		require.Equal(t, kknBytes, resSet[i*2+1])
	}

	resGet, resClosers, errGet := bdb.db.DKHMGet(rargs...)
	require.NoError(t, errGet)
	require.Equal(t, kn*(2+kkn), len(resGet))
	for i := range rres {
		require.Equal(t, rres[i], resGet[i])
	}
	for i := range resClosers {
		resClosers[i]()
	}

	resDel, errDel := bdb.db.DKHDel(dargs...)
	require.NoError(t, errDel)
	require.Equal(t, kn*2, len(resDel))
	for i := 0; i < kn; i++ {
		key := []byte(fmt.Sprintf("dkhash_%d", i))
		require.Equal(t, key, resDel[i*2])
		require.Equal(t, delBytes, resDel[i*2+1])
	}

	resGet, resClosers, errGet = bdb.db.DKHMGet(rargs...)
	require.NoError(t, errGet)
	require.Equal(t, kn*(2+kkn), len(resGet))
	for i := range rres {
		require.Equal(t, dres[i], resGet[i])
	}
	for i := range resClosers {
		resClosers[i]()
	}

	_, errDel = bdb.db.DKHDel(rargs...)
	require.NoError(t, errDel)
	for i := 0; i < kn; i++ {
		key := []byte(fmt.Sprintf("dkhash_%d", i))
		n, err := bdb.db.Exists(key, hash.Fnv32(key))
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
	}
}

func TestDKSet(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	var wargs, dargs [][]byte
	kn := 10
	kkn := 10
	kknBytes := extend.FormatIntToSlice(kkn)
	delBytes := extend.FormatIntToSlice(kkn / 2)
	for i := 0; i < kn; i++ {
		key := []byte(fmt.Sprintf("dkset_%d", i))
		wargs = append(wargs, key, kknBytes)
		dargs = append(dargs, key, delBytes)
		for j := 0; j < kkn; j++ {
			f := []byte(fmt.Sprintf("dksetfield_%d", j))
			wargs = append(wargs, f)
			if j%2 == 0 {
				dargs = append(dargs, f)
			}
		}
	}

	resSet, errSet := bdb.db.DKSAdd(wargs...)
	require.Equal(t, bitalosdb.ErrNotFound, errSet)

	for i := 0; i < kn; i++ {
		key := []byte(fmt.Sprintf("dkset_%d", i))
		err := bdb.db.DKCreateShard(btools.DataTypeSet, key)
		require.NoError(t, err)
	}

	resSet, errSet = bdb.db.DKSAdd(wargs...)
	require.NoError(t, errSet)
	require.Equal(t, kn*2, len(resSet))
	for i := 0; i < kn; i++ {
		key := []byte(fmt.Sprintf("dkset_%d", i))
		require.Equal(t, key, resSet[i*2])
		require.Equal(t, kknBytes, resSet[i*2+1])
	}

	for i := 0; i < kn; i++ {
		key := []byte(fmt.Sprintf("dkset_%d", i))
		for j := 0; j < kkn; j++ {
			f := []byte(fmt.Sprintf("dksetfield_%d", j))
			n, err := bdb.db.SIsMember(key, hash.Fnv32(key), f)
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
		}
	}

	resDel, errDel := bdb.db.DKSRem(dargs...)
	require.NoError(t, errDel)
	require.Equal(t, kn*2, len(resDel))
	for i := 0; i < kn; i++ {
		key := []byte(fmt.Sprintf("dkset_%d", i))
		require.Equal(t, key, resDel[i*2])
		require.Equal(t, delBytes, resDel[i*2+1])
	}

	for i := 0; i < kn; i++ {
		key := []byte(fmt.Sprintf("dkset_%d", i))
		for j := 0; j < kkn; j++ {
			f := []byte(fmt.Sprintf("dksetfield_%d", j))
			n, err := bdb.db.SIsMember(key, hash.Fnv32(key), f)
			require.NoError(t, err)
			if j%2 == 0 {
				require.Equal(t, int64(0), n)
			} else {
				require.Equal(t, int64(1), n)
			}
		}
	}

	for i := 0; i < kn; i++ {
		key := []byte(fmt.Sprintf("dkset_%d", i))
		if i%2 == 0 {
			var sr [][]byte
			sr = append(sr, key, kknBytes)
			for j := 0; j < kkn; j++ {
				sr = append(sr, []byte(fmt.Sprintf("dksetfield_%d", j)))
			}
			_, err := bdb.db.DKSRem(sr...)
			require.NoError(t, err)
		} else {
			_, closer, err := bdb.db.DKSPop(key, hash.Fnv32(key), int64(kkn))
			require.NoError(t, err)
			closer()
		}

		n, err := bdb.db.Exists(key, hash.Fnv32(key))
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
	}
}
