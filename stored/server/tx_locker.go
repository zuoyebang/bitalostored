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

package server

import (
	"sync"
	"sync/atomic"

	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
)

type TxLocker struct {
	sync.RWMutex
	txWatchKeys map[string]*TxWatchKey
}

type TxWatchKey struct {
	key       string
	txClients map[*Client]struct{}
	watched   atomic.Bool

	modifyTs atomic.Int64
	mu       sync.Mutex
}

type TxShardLocker struct {
	cap     uint32
	lockers []*TxLocker
}

func NewTxLockers(shards uint32) *TxShardLocker {
	sl := &TxShardLocker{}
	sl.lockers = make([]*TxLocker, 0, shards)
	for i := 0; i < int(shards); i++ {
		l := &TxLocker{}
		l.txWatchKeys = make(map[string]*TxWatchKey, 100)
		sl.lockers = append(sl.lockers, l)
	}
	sl.cap = shards
	return sl
}

func (sl *TxShardLocker) GetTxLock(khash uint32) *TxLocker {
	return sl.lockers[khash%sl.cap]
}

func (sl *TxShardLocker) GetTxLockByKey(key []byte) *TxLocker {
	return sl.lockers[hash.Fnv32(key)%sl.cap]
}

func (sl *TxShardLocker) GetWatchKeyWithKhash(khash uint32, keyStr string) *TxWatchKey {
	txLocker := sl.lockers[khash%sl.cap]
	txLocker.RLock()
	defer txLocker.RUnlock()
	return txLocker.txWatchKeys[keyStr]
}

func (sl *TxShardLocker) GetWatchKey(keyStr string) *TxWatchKey {
	khash := hash.Fnv32(unsafe2.ByteSlice(keyStr))
	return sl.GetWatchKeyWithKhash(khash, keyStr)
}

func (txLock *TxLocker) addWatchKey(c *Client, watchKey string, watched bool) (wk *TxWatchKey) {
	txLock.Lock()
	defer txLock.Unlock()
	var ok bool
	if wk, ok = txLock.txWatchKeys[watchKey]; ok {
		if _, ok2 := wk.txClients[c]; !ok2 {
			wk.txClients[c] = struct{}{}
		}
		if watched {
			wk.watched.CompareAndSwap(false, true)
		}
	} else {
		wk = &TxWatchKey{key: watchKey}
		wk.watched.Store(watched)
		wk.txClients = make(map[*Client]struct{}, 1)
		wk.txClients[c] = struct{}{}
		txLock.txWatchKeys[watchKey] = wk
	}
	return wk
}

func (txLock *TxLocker) removeWatchKey(c *Client, watchKey string) {
	txLock.Lock()
	defer txLock.Unlock()

	if wk, ok := txLock.txWatchKeys[watchKey]; ok {
		delete(wk.txClients, c)
		if len(wk.txClients) <= 0 {
			delete(txLock.txWatchKeys, watchKey)
		}
	}
}
