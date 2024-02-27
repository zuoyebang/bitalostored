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

package models

import (
	"github.com/zuoyebang/bitalostored/butils/trie"
)

const (
	LocalCachePrefix = "Local_Cache_Prefix_PC-"
	BlackKeys        = "Black_Keys"
)

const (
	LocalCacheIndex int = 0
	BlackKeysIndex  int = 1
)

type WhiteAndBlackList struct {
	White         []string        `json:"whitelist"`
	Black         []string        `json:"blacklist"`
	BlackMap      map[string]bool `json:"-"`
	WhiteMap      map[string]bool `json:"-"`
	WhitePrefixes []string        `json:"white_prefixes"`
	BlackPrefixes []string        `json:"black_prefixes"`
	WhiteTrie     trie.Trie       `json:"-"`
	BlackTrie     trie.Trie       `json:"-"`
}

type Pconfig struct {
	Name      string             `json:"name"`
	Remark    string             `json:"remark"`
	Content   *WhiteAndBlackList `json:"content"`
	OutOfSync bool               `json:"out_of_sync,omitempty"`
}

func (pc *Pconfig) BuildTrie() {
	pc.Content.WhiteTrie = trie.NewCharTrie(pc.Content.WhitePrefixes)
	pc.Content.BlackTrie = trie.NewCharTrie(pc.Content.BlackPrefixes)
}

func (pc *Pconfig) Encode() []byte {
	return jsonEncode(pc)
}
