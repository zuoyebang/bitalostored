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

package engine

import (
	"sync"

	"github.com/zuoyebang/bitalostored/stored/internal/resp"
)

type KeyLockerPool struct {
	cap     uint32
	lockers []*Locker
}

func NewKeyLockerPool() *KeyLockerPool {
	var keyLockerCap uint32 = 8 << 10
	lockers := make([]*Locker, 0, keyLockerCap)
	for i := 0; i < int(keyLockerCap); i++ {
		lockers = append(lockers, &Locker{})
	}
	return &KeyLockerPool{
		cap:     keyLockerCap,
		lockers: lockers,
	}
}

func (lp *KeyLockerPool) LockKey(khash uint32, cmd string) func() {
	index := khash % lp.cap
	return lp.lockers[index].DoLock(cmd)
}

type Locker struct {
	sync.RWMutex
}

func (c *Locker) DoLock(cmd string) func() {
	if resp.IsWriteCmd(cmd) {
		return c.getWLock()
	} else {
		return c.getRLock()
	}
}

func (c *Locker) getWLock() func() {
	c.Lock()
	return func() {
		c.Unlock()
	}
}

func (c *Locker) getRLock() func() {
	c.RLock()
	return func() {
		c.RUnlock()
	}
}
