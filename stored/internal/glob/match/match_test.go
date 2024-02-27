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

package match

import (
	"reflect"
	"testing"
	"unicode/utf8"
)

var bench_separators = []rune{'.'}

const bench_pattern = "abcdefghijklmnopqrstuvwxyz0123456789"

func TestAppendMerge(t *testing.T) {
	for id, test := range []struct {
		segments [2][]int
		exp      []int
	}{
		{
			[2][]int{
				{0, 6, 7},
				{0, 1, 3},
			},
			[]int{0, 1, 3, 6, 7},
		},
		{
			[2][]int{
				{0, 1, 3, 6, 7},
				{0, 1, 10},
			},
			[]int{0, 1, 3, 6, 7, 10},
		},
	} {
		act := appendMerge(test.segments[0], test.segments[1])
		if !reflect.DeepEqual(act, test.exp) {
			t.Errorf("#%d merge sort segments unexpected:\nact: %v\nexp:%v", id, act, test.exp)
			continue
		}
	}
}

func BenchmarkAppendMerge(b *testing.B) {
	s1 := []int{0, 1, 3, 6, 7}
	s2 := []int{0, 1, 3}

	for i := 0; i < b.N; i++ {
		appendMerge(s1, s2)
	}
}

func BenchmarkAppendMergeParallel(b *testing.B) {
	s1 := []int{0, 1, 3, 6, 7}
	s2 := []int{0, 1, 3}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			appendMerge(s1, s2)
		}
	})
}

func BenchmarkReverse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		reverseSegments([]int{1, 2, 3, 4})
	}
}

func getTable() []int {
	table := make([]int, utf8.MaxRune+1)
	for i := 0; i <= utf8.MaxRune; i++ {
		table[i] = utf8.RuneLen(rune(i))
	}

	return table
}

var table = getTable()

const runeToLen = 'q'

func BenchmarkRuneLenFromTable(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = table[runeToLen]
	}
}

func BenchmarkRuneLenFromUTF8(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = utf8.RuneLen(runeToLen)
	}
}
