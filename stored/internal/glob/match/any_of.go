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

import "fmt"

type AnyOf struct {
	Matchers Matchers
}

func NewAnyOf(m ...Matcher) AnyOf {
	return AnyOf{Matchers(m)}
}

func (self *AnyOf) Add(m Matcher) error {
	self.Matchers = append(self.Matchers, m)
	return nil
}

func (self AnyOf) Match(s string) bool {
	for _, m := range self.Matchers {
		if m.Match(s) {
			return true
		}
	}

	return false
}

func (self AnyOf) Index(s string) (int, []int) {
	index := -1

	segments := acquireSegments(len(s))
	for _, m := range self.Matchers {
		idx, seg := m.Index(s)
		if idx == -1 {
			continue
		}

		if index == -1 || idx < index {
			index = idx
			segments = append(segments[:0], seg...)
			continue
		}

		if idx > index {
			continue
		}

		segments = appendMerge(segments, seg)
	}

	if index == -1 {
		releaseSegments(segments)
		return -1, nil
	}

	return index, segments
}

func (self AnyOf) Len() (l int) {
	l = -1
	for _, m := range self.Matchers {
		ml := m.Len()
		switch {
		case l == -1:
			l = ml
			continue

		case ml == -1:
			return -1

		case l != ml:
			return -1
		}
	}

	return
}

func (self AnyOf) String() string {
	return fmt.Sprintf("<any_of:[%s]>", self.Matchers)
}
