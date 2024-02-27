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

package strings

import (
	"strings"
	"unicode/utf8"
)

func IndexAnyRunes(s string, rs []rune) int {
	for _, r := range rs {
		if i := strings.IndexRune(s, r); i != -1 {
			return i
		}
	}

	return -1
}

func LastIndexAnyRunes(s string, rs []rune) int {
	for _, r := range rs {
		i := -1
		if 0 <= r && r < utf8.RuneSelf {
			i = strings.LastIndexByte(s, byte(r))
		} else {
			sub := s
			for len(sub) > 0 {
				j := strings.IndexRune(s, r)
				if j == -1 {
					break
				}
				i = j
				sub = sub[i+1:]
			}
		}
		if i != -1 {
			return i
		}
	}
	return -1
}
