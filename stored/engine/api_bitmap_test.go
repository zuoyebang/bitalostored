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
	"math"
	"testing"

	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/stretchr/testify/require"
)

func TestKVSetBit64(t *testing.T) {
	bitsdb := testNewNoCacheBitsDB()
	defer bitsdb.Close()

	bdb := bitsdb.db
	bitKey := []byte("test_bitmap64_key")
	pos := math.MaxUint32 * 2
	khash := hash.Fnv32(bitKey)
	n, err := bdb.SetBit64(bitKey, khash, pos, 1)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
	n, err = bdb.SetBit64(bitKey, khash, pos, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, err = bdb.GetBit64(bitKey, khash, pos)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	n, err = bdb.GetBit64(bitKey, khash, pos+1)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	n, err = bdb.BitPos64(bitKey, khash, 1, 0, -1)
	require.NoError(t, err)
	require.Equal(t, int64(pos), n)

	n, err = bdb.BitCount64(bitKey, khash, 0, -1)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
}

func TestKVSetBitGetBit64(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	key := []byte("TestKVSetBitGetBit64")
	khash := hash.Fnv32(key)

	cases := []struct {
		offset, on     int
		setexp, getexp int64
	}{
		{0, 0, 0, 0},
		{0, 1, 0, 1},
		{0, 1, 1, 1},
		{123, 0, 0, 0},
		{123, 1, 0, 1},
		{123, 1, 1, 1},
		{1234, 1, 0, 1},
		{1234, 0, 1, 0},
		{1234, 0, 0, 0},
		{math.MaxInt64, 1, 0, 1},
		{math.MaxInt64, 0, 1, 0},
		{math.MaxInt64, 0, 0, 0},
	}

	t.Run("test setbit64 and getbit64", func(t *testing.T) {
		for _, c := range cases {
			n, err := bdb.db.SetBit64(key, khash, c.offset, c.on)
			require.NoError(t, err)
			require.Equal(t, c.setexp, n)

			n, err = bdb.db.GetBit64(key, khash, c.offset)
			require.NoError(t, err)
			require.Equal(t, c.getexp, n)
		}
	})
}

func TestKVBitCount64(t *testing.T) {
	bitsdb := testNewNoCacheBitsDB()
	defer bitsdb.Close()

	bdb := bitsdb.db
	key := []byte("TestKVBitCount64")
	khash := hash.Fnv32(key)

	n, err := bdb.BitCount64(key, khash, 0, -1)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	for i := 110; i <= 120; i++ {
		n, err = bdb.SetBit64(key, khash, i, 1)
		require.NoError(t, err)
		require.Equal(t, int64(0), n)
	}

	cases := []struct {
		start, end int
		exp        int64
	}{
		{0, -1, 11},
		{109, 130, 11},
		{109, 113, 4},
		{111, 113, 3},
		{109, 130, 11},
		{119, 130, 2},
		{129, 140, 0},
		{119, -2, 2},
		{-1, -10, 0},
		{1724947200, 1725292800, 0},
	}

	for _, c := range cases {
		n, err = bdb.BitCount64(key, khash, c.start, c.end)
		require.NoError(t, err)
		require.Equal(t, c.exp, n)
	}
}

func TestKVBitPos64(t *testing.T) {
	bitsdb := testNewNoCacheBitsDB()
	defer bitsdb.Close()

	bdb := bitsdb.db
	key := []byte("TestKVBitPos64")
	khash := hash.Fnv32(key)
	n, err := bdb.BitPos64(key, khash, 1, 0, -1)
	require.NoError(t, err)
	require.Equal(t, int64(-1), n)
	n, err = bdb.BitPos64(key, khash, 0, 0, -1)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	for i := 110; i <= 120; i++ {
		n, err = bdb.SetBit64(key, khash, i, 1)
		require.NoError(t, err)
		require.Equal(t, int64(0), n)
	}

	n, err = bdb.SetBit64(key, khash, 125, 1)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	cases := []struct {
		start, end int
		exp1, exp0 int64
	}{
		{0, -1, 110, 0},
		{109, 130, 110, 109},
		{109, 113, 110, 109},
		{110, 113, 110, -1},
		{110, 130, 110, 121},
		{109, 130, 110, 109},
		{119, 130, 119, 121},
		{129, 140, -1, -1},
		{119, -2, 119, 121},
		{-10, -1, -1, -1},
		{-1, -10, -1, -1},
	}

	for _, c := range cases {
		n, err = bdb.BitPos64(key, khash, 1, c.start, c.end)
		require.NoError(t, err)
		require.Equal(t, c.exp1, n)

		n, err = bdb.BitPos64(key, khash, 0, c.start, c.end)
		require.NoError(t, err)
		require.Equal(t, c.exp0, n)
	}
}
