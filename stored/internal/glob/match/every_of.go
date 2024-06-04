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

type EveryOf struct {
	Matchers Matchers
}

func NewEveryOf(m ...Matcher) EveryOf {
	return EveryOf{Matchers(m)}
}

func (self *EveryOf) Add(m Matcher) error {
	self.Matchers = append(self.Matchers, m)
	return nil
}

func (self EveryOf) Len() (l int) {
	for _, m := range self.Matchers {
		if ml := m.Len(); l > 0 {
			l += ml
		} else {
			return -1
		}
	}

	return
}

func (self EveryOf) Index(s string) (int, []int) {
	var index int
	var offset int

	next := acquireSegments(len(s))
	current := acquireSegments(len(s))

	sub := s
	for i, m := range self.Matchers {
		idx, seg := m.Index(sub)
		if idx == -1 {
			releaseSegments(next)
			releaseSegments(current)
			return -1, nil
		}

		if i == 0 {
			// we use copy here instead of `current = seg`
			// cause seg is a slice from reusable buffer `in`
			// and it could be overwritten in next iteration
			current = append(current, seg...)
		} else {
			// clear the next
			next = next[:0]

			delta := index - (idx + offset)
			for _, ex := range current {
				for _, n := range seg {
					if ex+delta == n {
						next = append(next, n)
					}
				}
			}

			if len(next) == 0 {
				releaseSegments(next)
				releaseSegments(current)
				return -1, nil
			}

			current = append(current[:0], next...)
		}

		index = idx + offset
		sub = s[index:]
		offset += idx
	}

	releaseSegments(next)

	return index, current
}

func (self EveryOf) Match(s string) bool {
	for _, m := range self.Matchers {
		if !m.Match(s) {
			return false
		}
	}

	return true
}

func (self EveryOf) String() string {
	return fmt.Sprintf("<every_of:[%s]>", self.Matchers)
}
