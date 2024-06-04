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

package match

import (
	"reflect"
	"testing"
)

func TestPrefixSuffixIndex(t *testing.T) {
	for id, test := range []struct {
		prefix   string
		suffix   string
		fixture  string
		index    int
		segments []int
	}{
		{
			"a",
			"c",
			"abc",
			0,
			[]int{3},
		},
		{
			"f",
			"f",
			"fffabfff",
			0,
			[]int{1, 2, 3, 6, 7, 8},
		},
		{
			"ab",
			"bc",
			"abc",
			0,
			[]int{3},
		},
	} {
		p := NewPrefixSuffix(test.prefix, test.suffix)
		index, segments := p.Index(test.fixture)
		if index != test.index {
			t.Errorf("#%d unexpected index: exp: %d, act: %d", id, test.index, index)
		}
		if !reflect.DeepEqual(segments, test.segments) {
			t.Errorf("#%d unexpected segments: exp: %v, act: %v", id, test.segments, segments)
		}
	}
}

func BenchmarkIndexPrefixSuffix(b *testing.B) {
	m := NewPrefixSuffix("qew", "sqw")

	for i := 0; i < b.N; i++ {
		_, s := m.Index(bench_pattern)
		releaseSegments(s)
	}
}

func BenchmarkIndexPrefixSuffixParallel(b *testing.B) {
	m := NewPrefixSuffix("qew", "sqw")

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, s := m.Index(bench_pattern)
			releaseSegments(s)
		}
	})
}
