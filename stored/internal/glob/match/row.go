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
)

type Row struct {
	Matchers    Matchers
	RunesLength int
	Segments    []int
}

func NewRow(len int, m ...Matcher) Row {
	return Row{
		Matchers:    Matchers(m),
		RunesLength: len,
		Segments:    []int{len},
	}
}

func (self Row) matchAll(s string) bool {
	var idx int
	for _, m := range self.Matchers {
		length := m.Len()

		var next, i int
		for next = range s[idx:] {
			i++
			if i == length {
				break
			}
		}

		if i < length || !m.Match(s[idx:idx+next+1]) {
			return false
		}

		idx += next + 1
	}

	return true
}

func (self Row) lenOk(s string) bool {
	var i int
	for range s {
		i++
		if i > self.RunesLength {
			return false
		}
	}
	return self.RunesLength == i
}

func (self Row) Match(s string) bool {
	return self.lenOk(s) && self.matchAll(s)
}

func (self Row) Len() (l int) {
	return self.RunesLength
}

func (self Row) Index(s string) (int, []int) {
	for i := range s {
		if len(s[i:]) < self.RunesLength {
			break
		}
		if self.matchAll(s[i:]) {
			return i, self.Segments
		}
	}
	return -1, nil
}

func (self Row) String() string {
	return fmt.Sprintf("<row_%d:[%s]>", self.RunesLength, self.Matchers)
}
