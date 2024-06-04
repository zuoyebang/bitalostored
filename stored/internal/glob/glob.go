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

package glob

import (
	"github.com/zuoyebang/bitalostored/stored/internal/glob/compiler"
	"github.com/zuoyebang/bitalostored/stored/internal/glob/syntax"
)

type Glob interface {
	Match(string) bool
}

// Compile creates Glob for given pattern and strings (if any present after pattern) as separators.
// The pattern syntax is:
//
//	pattern:
//	    { term }
//
//	term:
//	    `*`         matches any sequence of non-separator characters
//	    `**`        matches any sequence of characters
//	    `?`         matches any single non-separator character
//	    `[` [ `!` ] { character-range } `]`
//	                character class (must be non-empty)
//	    `{` pattern-list `}`
//	                pattern alternatives
//	    c           matches character c (c != `*`, `**`, `?`, `\`, `[`, `{`, `}`)
//	    `\` c       matches character c
//
//	character-range:
//	    c           matches character c (c != `\\`, `-`, `]`)
//	    `\` c       matches character c
//	    lo `-` hi   matches character c for lo <= c <= hi
//
//	pattern-list:
//	    pattern { `,` pattern }
//	                comma-separated (without spaces) patterns
func Compile(pattern string, separators ...rune) (Glob, error) {
	ast, err := syntax.Parse(pattern)
	if err != nil {
		return nil, err
	}

	matcher, err := compiler.Compile(ast, separators)
	if err != nil {
		return nil, err
	}

	return matcher, nil
}

func MustCompile(pattern string, separators ...rune) Glob {
	g, err := Compile(pattern, separators...)
	if err != nil {
		panic(err)
	}

	return g
}

func QuoteMeta(s string) string {
	b := make([]byte, 2*len(s))

	j := 0
	for i := 0; i < len(s); i++ {
		if syntax.Special(s[i]) {
			b[j] = '\\'
			j++
		}
		b[j] = s[i]
		j++
	}

	return string(b[0:j])
}
