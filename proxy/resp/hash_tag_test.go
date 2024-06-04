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

package resp

import (
	"bytes"
	"hash/crc32"
	"testing"

	"github.com/zuoyebang/bitalostored/butils/hash"

	"github.com/stretchr/testify/require"
)

func Hash(key []byte) uint32 {
	const (
		TagBeg = '{'
		TagEnd = '}'
	)
	if beg := bytes.IndexByte(key, TagBeg); beg >= 0 {
		if end := bytes.IndexByte(key[beg+1:], TagEnd); end >= 0 {
			key = key[beg+1 : beg+1+end]
		}
	}
	return crc32.ChecksumIEEE(key)
}

func TestExtractHash(t *testing.T) {
	var m = map[string]string{
		"{abc}":           "abc",
		"{{{abc1}abc2}":   "{{abc1",
		"abc1{abc2{abc3}": "abc2{abc3",
		//"{{{{abc":         "{{{{abc",
		//"{{{{abc}":        "{{{abc",
		//"{{}{{abc":        "{",
		//"abc}{abc":        "abc}{abc",
		//"abc}{123}456":    "123",
		//"123{abc}456":     "abc",
		//"{}abc":           "",
		//"abc{}123":        "",
		//"123{456}":        "456",
	}
	for k, v := range m {
		i := Hash([]byte(k))
		j := Hash([]byte(v))
		require.Equal(t, i, j)
	}
}

func TestExtractHashTag(t *testing.T) {
	var m = map[string]string{
		//"{abc}":           "abc",
		"{{{abc1}abc2}": "{{abc1",
		//"abc1{abc2{abc3}": "abc2{abc3",
		//"{{{{abc":         "{{{{abc",
		//"{{{{abc}":        "{{{abc",
		//"{{}{{abc":        "{",
		//"abc}{abc":        "abc}{abc",
		//"abc}{123}456":    "123",
		//"123{abc}456":     "abc",
		//"{}abc":           "",
		//"abc{}123":        "",
		//"123{456}":        "456",
	}
	for k, v := range m {
		i := ExtractHashTag(k)
		j := ExtractHashTag(v)
		require.Equal(t, hash.Fnv32(i), hash.Fnv32(j))
	}
}
