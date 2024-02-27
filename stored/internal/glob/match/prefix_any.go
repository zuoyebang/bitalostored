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

	sutil "github.com/zuoyebang/bitalostored/stored/internal/glob/util/strings"
)

type PrefixAny struct {
	Prefix     string
	Separators []rune
}

func NewPrefixAny(s string, sep []rune) PrefixAny {
	return PrefixAny{s, sep}
}

func (self PrefixAny) Index(s string) (int, []int) {
	idx := strings.Index(s, self.Prefix)
	if idx == -1 {
		return -1, nil
	}

	n := len(self.Prefix)
	sub := s[idx+n:]
	i := sutil.IndexAnyRunes(sub, self.Separators)
	if i > -1 {
		sub = sub[:i]
	}

	seg := acquireSegments(len(sub) + 1)
	seg = append(seg, n)
	for i, r := range sub {
		seg = append(seg, n+i+utf8.RuneLen(r))
	}

	return idx, seg
}

func (self PrefixAny) Len() int {
	return lenNo
}

func (self PrefixAny) Match(s string) bool {
	if !strings.HasPrefix(s, self.Prefix) {
		return false
	}
	return sutil.IndexAnyRunes(s[len(self.Prefix):], self.Separators) == -1
}

func (self PrefixAny) String() string {
	return fmt.Sprintf("<prefix_any:%s![%s]>", self.Prefix, string(self.Separators))
}
