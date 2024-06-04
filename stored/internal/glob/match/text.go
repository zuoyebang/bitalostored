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
	"strings"
	"unicode/utf8"
)

type Text struct {
	Str         string
	RunesLength int
	BytesLength int
	Segments    []int
}

func NewText(s string) Text {
	return Text{
		Str:         s,
		RunesLength: utf8.RuneCountInString(s),
		BytesLength: len(s),
		Segments:    []int{len(s)},
	}
}

func (self Text) Match(s string) bool {
	return self.Str == s
}

func (self Text) Len() int {
	return self.RunesLength
}

func (self Text) Index(s string) (int, []int) {
	index := strings.Index(s, self.Str)
	if index == -1 {
		return -1, nil
	}

	return index, self.Segments
}

func (self Text) String() string {
	return fmt.Sprintf("<text:`%v`>", self.Str)
}
