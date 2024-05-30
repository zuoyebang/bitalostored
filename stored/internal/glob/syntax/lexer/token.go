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

package lexer

import "fmt"

type TokenType int

const (
	EOF TokenType = iota
	Error
	Text
	Char
	Any
	Super
	Single
	Not
	Separator
	RangeOpen
	RangeClose
	RangeLo
	RangeHi
	RangeBetween
	TermsOpen
	TermsClose
)

func (tt TokenType) String() string {
	switch tt {
	case EOF:
		return "eof"

	case Error:
		return "error"

	case Text:
		return "text"

	case Char:
		return "char"

	case Any:
		return "any"

	case Super:
		return "super"

	case Single:
		return "single"

	case Not:
		return "not"

	case Separator:
		return "separator"

	case RangeOpen:
		return "range_open"

	case RangeClose:
		return "range_close"

	case RangeLo:
		return "range_lo"

	case RangeHi:
		return "range_hi"

	case RangeBetween:
		return "range_between"

	case TermsOpen:
		return "terms_open"

	case TermsClose:
		return "terms_close"

	default:
		return "undef"
	}
}

type Token struct {
	Type TokenType
	Raw  string
}

func (t Token) String() string {
	return fmt.Sprintf("%v<%q>", t.Type, t.Raw)
}
