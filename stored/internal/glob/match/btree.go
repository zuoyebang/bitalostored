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

type BTree struct {
	Value            Matcher
	Left             Matcher
	Right            Matcher
	ValueLengthRunes int
	LeftLengthRunes  int
	RightLengthRunes int
	LengthRunes      int
}

func NewBTree(Value, Left, Right Matcher) (tree BTree) {
	tree.Value = Value
	tree.Left = Left
	tree.Right = Right

	lenOk := true
	if tree.ValueLengthRunes = Value.Len(); tree.ValueLengthRunes == -1 {
		lenOk = false
	}

	if Left != nil {
		if tree.LeftLengthRunes = Left.Len(); tree.LeftLengthRunes == -1 {
			lenOk = false
		}
	}

	if Right != nil {
		if tree.RightLengthRunes = Right.Len(); tree.RightLengthRunes == -1 {
			lenOk = false
		}
	}

	if lenOk {
		tree.LengthRunes = tree.LeftLengthRunes + tree.ValueLengthRunes + tree.RightLengthRunes
	} else {
		tree.LengthRunes = -1
	}

	return tree
}

func (self BTree) Len() int {
	return self.LengthRunes
}

func (self BTree) Index(s string) (index int, segments []int) {
	return -1, nil
}

func (self BTree) Match(s string) bool {
	inputLen := len(s)
	offset, limit := self.offsetLimit(inputLen)

	for offset < limit {
		index, segments := self.Value.Index(s[offset:limit])
		if index == -1 {
			releaseSegments(segments)
			return false
		}

		l := s[:offset+index]
		var left bool
		if self.Left != nil {
			left = self.Left.Match(l)
		} else {
			left = l == ""
		}

		if left {
			for i := len(segments) - 1; i >= 0; i-- {
				length := segments[i]

				var right bool
				var r string
				if inputLen <= offset+index+length {
					r = ""
				} else {
					r = s[offset+index+length:]
				}

				if self.Right != nil {
					right = self.Right.Match(r)
				} else {
					right = r == ""
				}

				if right {
					releaseSegments(segments)
					return true
				}
			}
		}

		_, step := utf8.DecodeRuneInString(s[offset+index:])
		offset += index + step

		releaseSegments(segments)
	}

	return false
}

func (self BTree) offsetLimit(inputLen int) (offset int, limit int) {
	if self.LengthRunes != -1 && self.LengthRunes > inputLen {
		return 0, 0
	}
	if self.LeftLengthRunes >= 0 {
		offset = self.LeftLengthRunes
	}
	if self.RightLengthRunes >= 0 {
		limit = inputLen - self.RightLengthRunes
	} else {
		limit = inputLen
	}
	return offset, limit
}

func (self BTree) String() string {
	const n string = "<nil>"
	var l, r string
	if self.Left == nil {
		l = n
	} else {
		l = self.Left.String()
	}
	if self.Right == nil {
		r = n
	} else {
		r = self.Right.String()
	}

	return fmt.Sprintf("<btree:[%s<-%s->%s]>", l, self.Value, r)
}
