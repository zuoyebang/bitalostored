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

package trie

import (
	"fmt"
	"strings"
	"sync"
)

type Trie interface {
	HasPrefix(key string) bool
	AddPrefix(prefix string)
	RemovePrefix(prefix string)
	print()
}

func NewCharTrie(prefixes []string) Trie {
	var lt LockTrie
	var ct CharTrie
	ct.children = make(map[string]*CharTrie, 26)
	ct.isLast = true
	lt.ct = &ct
	for _, prefix := range prefixes {
		lt.AddPrefix(prefix)
	}
	return &lt
}

type LockTrie struct {
	ct   Trie
	lock sync.RWMutex
}

func (lt *LockTrie) HasPrefix(key string) bool {
	lt.lock.RLock()
	hasPrefix := lt.ct.HasPrefix(key)
	lt.lock.RUnlock()
	return hasPrefix
}

func (lt *LockTrie) AddPrefix(prefix string) {
	lt.lock.Lock()
	lt.ct.AddPrefix(prefix)
	lt.lock.Unlock()
}

func (lt *LockTrie) RemovePrefix(prefix string) {
	lt.lock.Lock()
	lt.ct.RemovePrefix(prefix)
	lt.lock.Unlock()
}

func (lt *LockTrie) print() {
	lt.ct.print()
}

type CharTrie struct {
	isLast   bool
	children map[string]*CharTrie
}

func (ct *CharTrie) HasPrefix(key string) bool {
	if ct.isLast {
		return false
	}
	var child *CharTrie
	var ok bool
	child = ct
	for index, _ := range key {
		if child.isLast {
			return true
		}
		if child, ok = child.children[key[index:index+1]]; !ok {
			return false
		}
	}

	if child.isLast {
		return true
	}
	return false
}

func (ct *CharTrie) AddPrefix(prefix string) {
	prefix = strings.TrimSpace(prefix)
	if len(prefix) == 0 {
		return
	}
	child := ct
	for index, _ := range prefix {
		if _, ok := child.children[prefix[index:index+1]]; ok {
			child = child.children[prefix[index:index+1]]
			continue
		} else {
			child.isLast = false
			child.children[prefix[index:index+1]] = &CharTrie{
				isLast:   true,
				children: make(map[string]*CharTrie, 26),
			}
			child = child.children[prefix[index:index+1]]
		}
	}
	return
}

func (ct *CharTrie) print() {
	prefixes = []string{}
	ct.allPrefixes("")
	fmt.Println(prefixes)

}

var prefixes []string

func (ct *CharTrie) allPrefixes(prefix string) {
	if ct.isLast {
		prefixes = append(prefixes, prefix)
	}
	for key, _ := range ct.children {
		prefix = prefix + key
		ct.children[key].allPrefixes(prefix)
		prefix = prefix[:len(prefix)-1]
	}
}

func (ct *CharTrie) RemovePrefix(prefix string) {
	if len(ct.children) == 0 {
		return
	}
	prefix = strings.TrimSpace(prefix)
	if len(prefix) == 0 {
		return
	}
	if _, ok := ct.children[prefix[0:1]]; !ok {
		return
	}
	branchIndex := 0
	branchNode := ct
	trimFlag := false
	child := ct.children[prefix[0:1]]
	for index := 1; index < len(prefix); index++ {
		if _, ok := child.children[prefix[index:index+1]]; !ok {
			return
		}

		if len(child.children) > 1 {
			branchNode = child
			branchIndex = index
		}
		child = child.children[prefix[index:index+1]]
		if child.isLast && index == len(prefix)-1 {
			trimFlag = true
		}
	}
	if trimFlag {
		delete(branchNode.children, prefix[branchIndex:branchIndex+1])
	}
	return
}
