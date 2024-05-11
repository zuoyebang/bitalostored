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

package locker

import (
	"sync"
)

type locker struct {
	sync.RWMutex
}

func (c *locker) getWLock() func() {
	c.Lock()
	return func() {
		c.Unlock()
	}
}

func (c *locker) getRLock() func() {
	c.RLock()
	return func() {
		c.RUnlock()
	}
}

type ScopeLocker struct {
	cap     uint32
	lockers []*locker
}

func NewScopeLocker(num uint32) *ScopeLocker {
	lockers := make([]*locker, 0, num)
	for i := 0; i < int(num); i++ {
		lockers = append(lockers, &locker{})
	}
	return &ScopeLocker{
		cap:     num,
		lockers: lockers,
	}
}

func (sl *ScopeLocker) LockWriteKey(khash uint32) func() {
	return sl.lockers[khash%sl.cap].getWLock()
}

func (sl *ScopeLocker) LockReadKey(khash uint32) func() {
	return sl.lockers[khash%sl.cap].getRLock()
}
