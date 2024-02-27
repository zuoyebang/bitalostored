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

	"github.com/zuoyebang/bitalostored/stored/internal/glob/util/runes"
)

type Single struct {
	Separators []rune
}

func NewSingle(s []rune) Single {
	return Single{s}
}

func (self Single) Match(s string) bool {
	r, w := utf8.DecodeRuneInString(s)
	if len(s) > w {
		return false
	}

	return runes.IndexRune(self.Separators, r) == -1
}

func (self Single) Len() int {
	return lenOne
}

func (self Single) Index(s string) (int, []int) {
	for i, r := range s {
		if runes.IndexRune(self.Separators, r) == -1 {
			return i, segmentsByRuneLength[utf8.RuneLen(r)]
		}
	}

	return -1, nil
}

func (self Single) String() string {
	return fmt.Sprintf("<single:![%s]>", string(self.Separators))
}
