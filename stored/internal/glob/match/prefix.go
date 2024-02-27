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
	"strings"
	"unicode/utf8"
)

type Prefix struct {
	Prefix string
}

func NewPrefix(p string) Prefix {
	return Prefix{p}
}

func (self Prefix) Index(s string) (int, []int) {
	idx := strings.Index(s, self.Prefix)
	if idx == -1 {
		return -1, nil
	}

	length := len(self.Prefix)
	var sub string
	if len(s) > idx+length {
		sub = s[idx+length:]
	} else {
		sub = ""
	}

	segments := acquireSegments(len(sub) + 1)
	segments = append(segments, length)
	for i, r := range sub {
		segments = append(segments, length+i+utf8.RuneLen(r))
	}

	return idx, segments
}

func (self Prefix) Len() int {
	return lenNo
}

func (self Prefix) Match(s string) bool {
	return strings.HasPrefix(s, self.Prefix)
}

func (self Prefix) String() string {
	return fmt.Sprintf("<prefix:%s>", self.Prefix)
}
