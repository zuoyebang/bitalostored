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

func TestEveryOfIndex(t *testing.T) {
	for id, test := range []struct {
		matchers Matchers
		fixture  string
		index    int
		segments []int
	}{
		{
			Matchers{
				NewAny(nil),
				NewText("b"),
				NewText("c"),
			},
			"dbc",
			-1,
			nil,
		},
		{
			Matchers{
				NewAny(nil),
				NewPrefix("b"),
				NewSuffix("c"),
			},
			"abc",
			1,
			[]int{2},
		},
	} {
		everyOf := NewEveryOf(test.matchers...)
		index, segments := everyOf.Index(test.fixture)
		if index != test.index {
			t.Errorf("#%d unexpected index: exp: %d, act: %d", id, test.index, index)
		}
		if !reflect.DeepEqual(segments, test.segments) {
			t.Errorf("#%d unexpected segments: exp: %v, act: %v", id, test.segments, segments)
		}
	}
}
