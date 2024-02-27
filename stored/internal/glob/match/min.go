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
	"fmt"
	"unicode/utf8"
)

type Min struct {
	Limit int
}

func NewMin(l int) Min {
	return Min{l}
}

func (self Min) Match(s string) bool {
	var l int
	for range s {
		l += 1
		if l >= self.Limit {
			return true
		}
	}

	return false
}

func (self Min) Index(s string) (int, []int) {
	var count int

	c := len(s) - self.Limit + 1
	if c <= 0 {
		return -1, nil
	}

	segments := acquireSegments(c)
	for i, r := range s {
		count++
		if count >= self.Limit {
			segments = append(segments, i+utf8.RuneLen(r))
		}
	}

	if len(segments) == 0 {
		return -1, nil
	}

	return 0, segments
}

func (self Min) Len() int {
	return lenNo
}

func (self Min) String() string {
	return fmt.Sprintf("<min:%d>", self.Limit)
}
