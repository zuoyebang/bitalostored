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

package vectormap

import (
	"math/bits"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestMatchMetadata(t *testing.T) {
	var meta metadata
	for i := range meta {
		meta[i] = int8(i)
	}
	t.Run("metaMatchH2", func(t *testing.T) {
		for _, x := range meta {
			mask := metaMatchH2(&meta, h2(x))
			assert.NotZero(t, mask)
			assert.Equal(t, uint32(x), nextMatch(&mask))
		}
	})
	t.Run("metaMatchEmpty", func(t *testing.T) {
		mask := metaMatchEmpty(&meta)
		assert.Equal(t, mask, bitset(0))
		for i := range meta {
			meta[i] = empty
			mask = metaMatchEmpty(&meta)
			assert.NotZero(t, mask)
			assert.Equal(t, uint32(i), nextMatch(&mask))
			meta[i] = int8(i)
		}
	})
	t.Run("nextMatch", func(t *testing.T) {
		// test iterating multiple matches
		meta = newEmptyMetadata()
		mask := metaMatchEmpty(&meta)
		for i := range meta {
			assert.Equal(t, uint32(i), nextMatch(&mask))
		}
		for i := 0; i < len(meta); i += 2 {
			meta[i] = int8(42)
		}
		mask = metaMatchH2(&meta, h2(42))
		for i := 0; i < len(meta); i += 2 {
			assert.Equal(t, uint32(i), nextMatch(&mask))
		}
	})
}

func BenchmarkMatchMetadata(b *testing.B) {
	var meta metadata
	for i := range meta {
		meta[i] = int8(i)
	}
	var mask bitset
	for i := 0; i < b.N; i++ {
		mask = metaMatchH2(&meta, h2(i))
	}
	b.Log(mask)
}

func TestNextPow2(t *testing.T) {
	assert.Equal(t, 0, int(nextPow2(0)))
	assert.Equal(t, 1, int(nextPow2(1)))
	assert.Equal(t, 2, int(nextPow2(2)))
	assert.Equal(t, 4, int(nextPow2(3)))
	assert.Equal(t, 8, int(nextPow2(7)))
	assert.Equal(t, 8, int(nextPow2(8)))
	assert.Equal(t, 16, int(nextPow2(9)))
}

func nextPow2(x uint32) uint32 {
	return 1 << (32 - bits.LeadingZeros32(x-1))
}

func TestConstants(t *testing.T) {
	c1, c2 := empty, tombstone
	assert.Equal(t, byte(0b1000_0000), byte(c1))
	assert.Equal(t, byte(0b1000_0000), reinterpretCast(c1))
	assert.Equal(t, byte(0b1111_1110), byte(c2))
	assert.Equal(t, byte(0b1111_1110), reinterpretCast(c2))
}

func reinterpretCast(i int8) byte {
	return *(*byte)(unsafe.Pointer(&i))
}

func TestFastMod(t *testing.T) {
	t.Run("n=10", func(t *testing.T) {
		testFastMod(t, 10)
	})
	t.Run("n=100", func(t *testing.T) {
		testFastMod(t, 100)
	})
	t.Run("n=1000", func(t *testing.T) {
		testFastMod(t, 1000)
	})
}

func testFastMod(t *testing.T, n uint32) {
	const trials = 32 * 1024
	for i := 0; i < trials; i++ {
		x := rand.Uint32()
		y := fastModN(x, n)
		assert.Less(t, y, n)
		t.Logf("fastMod(%d, %d): %d", x, n, y)
	}
}
