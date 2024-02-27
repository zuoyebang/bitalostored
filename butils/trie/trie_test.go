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

package trie

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAddPrefixCase1(t *testing.T) {
	clt := NewCharTrie([]string{"test"})
	assert.True(t, clt.HasPrefix("test1"))
	assert.False(t, clt.HasPrefix("tes"))
	assert.True(t, clt.HasPrefix("test"))
	clt.RemovePrefix("test")
	clt.print()
	assert.False(t, clt.HasPrefix("test1"))
	assert.False(t, clt.HasPrefix("tes"))
	assert.False(t, clt.HasPrefix("test"))
}

func TestAddPrefixCase2(t *testing.T) {
	clt := NewCharTrie([]string{"test", "testing"})
	assert.False(t, clt.HasPrefix("test1"))
	assert.True(t, clt.HasPrefix("testing1"))
	assert.False(t, clt.HasPrefix("test"))
	clt.print()
}

func TestAddPrefixCase3(t *testing.T) {
	clt := NewCharTrie([]string{"test", "testing", "testong"})
	assert.False(t, clt.HasPrefix("testo"))
	assert.True(t, clt.HasPrefix("testing1"))
	assert.True(t, clt.HasPrefix("testong1"))
	assert.False(t, clt.HasPrefix("testng"))
	clt.print()
}

func TestAddPrefixCase4(t *testing.T) {
	clt := NewCharTrie([]string{"test", "testing", "testong"})
	clt.RemovePrefix("test")
	assert.False(t, clt.HasPrefix("testo"))
	assert.True(t, clt.HasPrefix("testing1"))
	clt.RemovePrefix("testong")
	assert.False(t, clt.HasPrefix("testong1"))
	clt.RemovePrefix("testng")
	assert.False(t, clt.HasPrefix("testng"))
	clt.print()
}

func TestAddPrefix(t *testing.T) {
	ct := NewCharTrie([]string{"hahah", "asufhis", "ksdfskjf", "skdfhksdjfhks", "ksvksjdvhiksjd", "kfhksddmxcmvnskvjk", "ksdhdcvksjksj", "nzxmhkaioao", "skdfhsdkjfksjvnjsdkvn"})
	ct.print()
	fmt.Println(ct.HasPrefix("hahah"), ct.HasPrefix("haha"), ct.HasPrefix("hahahh"))
	ct.RemovePrefix("hahahh")
	fmt.Println(ct.HasPrefix("hahah"), ct.HasPrefix("haha"), ct.HasPrefix("hahahh"))
	ct.RemovePrefix("haha")
	fmt.Println(ct.HasPrefix("hahah"), ct.HasPrefix("haha"), ct.HasPrefix("hahahh"))
	ct.RemovePrefix("hahah")
	fmt.Println(ct.HasPrefix("hahah"), ct.HasPrefix("haha"), ct.HasPrefix("hahahh"))
	ct.AddPrefix("hello")
	ct.AddPrefix("world")
	ct.AddPrefix("word")
	ct.print()
}

func lockFreeCharTrie(prefixes []string) Trie {
	var ct CharTrie
	ct.children = make(map[string]*CharTrie, 26)
	ct.isLast = true
	for _, prefix := range prefixes {
		ct.AddPrefix(prefix)
	}
	return &ct
}

func TestRoutineTrie(t *testing.T) {
	ct := NewCharTrie([]string{"hahah", "asufhis", "ksdfskjf", "skdfhksdjfhks", "ksvksjdvhiksjd", "kfhksddmxcmvnskvjk", "ksdhdcvksjksj", "nzxmhkaioao", "skdfhsdkjfksjvnjsdkvn"})
	for i := 0; i <= 1000000; i++ {
		go ct.HasPrefix("skdfhksdjfhks")
	}
	for i := 0; i <= 1000; i++ {
		go ct.AddPrefix("world" + strconv.Itoa(i))
	}
	time.Sleep(time.Millisecond * 2)
	for i := 0; i <= 1000; i++ {
		go ct.RemovePrefix("world" + strconv.Itoa(i))
	}
	time.Sleep(time.Second * 2)
	ct.print()
}
