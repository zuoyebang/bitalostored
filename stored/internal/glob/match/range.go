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
	"fmt"
	"unicode/utf8"
)

type Range struct {
	Lo, Hi rune
	Not    bool
}

func NewRange(lo, hi rune, not bool) Range {
	return Range{lo, hi, not}
}

func (self Range) Len() int {
	return lenOne
}

func (self Range) Match(s string) bool {
	r, w := utf8.DecodeRuneInString(s)
	if len(s) > w {
		return false
	}

	inRange := r >= self.Lo && r <= self.Hi

	return inRange == !self.Not
}

func (self Range) Index(s string) (int, []int) {
	for i, r := range s {
		if self.Not != (r >= self.Lo && r <= self.Hi) {
			return i, segmentsByRuneLength[utf8.RuneLen(r)]
		}
	}

	return -1, nil
}

func (self Range) String() string {
	var not string
	if self.Not {
		not = "!"
	}
	return fmt.Sprintf("<range:%s[%s,%s]>", not, string(self.Lo), string(self.Hi))
}
