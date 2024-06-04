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

package models

import "github.com/zuoyebang/bitalostored/butils/trie"

const (
	Local_Cache_Prefix = "Local_Cache_Prefix_PC-"
	Black_Keys         = "Black_Keys"
)

var DefaultPconfigKeyList = map[string]*Pconfig{
	Local_Cache_Prefix: {
		Name:   Local_Cache_Prefix,
		Remark: "Black and white list configuration: Proxy local cache",
		Content: &WhiteAndBlackList{
			White:         []string{},
			Black:         []string{},
			WhitePrefixes: []string{},
			BlackPrefixes: []string{},
			WhiteTrie:     trie.NewCharTrie([]string{}),
			BlackTrie:     trie.NewCharTrie([]string{}),
		},
		OutOfSync: true,
	},
	Black_Keys: {
		Name:   Black_Keys,
		Remark: "Black and white list configuration: Block key",
		Content: &WhiteAndBlackList{
			White:         []string{},
			Black:         []string{},
			WhitePrefixes: []string{},
			BlackPrefixes: []string{},
			WhiteTrie:     trie.NewCharTrie([]string{}),
			BlackTrie:     trie.NewCharTrie([]string{}),
		},
		OutOfSync: true,
	},
}

type WhiteAndBlackList struct {
	White    []string        `json:"whitelist"`
	Black    []string        `json:"blacklist"`
	BlackMap map[string]bool `json:"-"`
	WhiteMap map[string]bool `json:"-"`

	WhitePrefixes []string  `json:"white_prefixes"`
	BlackPrefixes []string  `json:"black_prefixes"`
	WhiteTrie     trie.Trie `json:"-"`
	BlackTrie     trie.Trie `json:"-"`
}

func (pc *Pconfig) BuildTrie() {
	pc.Content.WhiteTrie = trie.NewCharTrie(pc.Content.WhitePrefixes)
	pc.Content.BlackTrie = trie.NewCharTrie(pc.Content.BlackPrefixes)
}

type Pconfig struct {
	Name      string             `json:"name"`
	Remark    string             `json:"remark"`
	Content   *WhiteAndBlackList `json:"content"`
	OutOfSync bool               `json:"out_of_sync,omitempty"`
}

func (p *Pconfig) Encode() []byte {
	return jsonEncode(p)
}
