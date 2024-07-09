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

package locker

import (
	"sync"

	"github.com/zuoyebang/bitalostored/stored/internal/resp"
)

const (
	lockerPoolSizeNormal uint32 = 4 << 10
	lockerPoolSizeLarge  uint32 = 16 << 10
)

type locker struct {
	sync.RWMutex
}

func (l *locker) getWLock() func() {
	l.Lock()
	return func() {
		l.Unlock()
	}
}

func (l *locker) getRLock() func() {
	l.RLock()
	return func() {
		l.RUnlock()
	}
}

func (l *locker) lockKey(cmd string) func() {
	if resp.IsWriteCmd(cmd) {
		return l.getWLock()
	} else {
		return l.getRLock()
	}
}

type ScopeLocker struct {
	size    uint32
	lockers []*locker
}

func NewScopeLocker(large bool) *ScopeLocker {
	size := lockerPoolSizeNormal
	if large {
		size = lockerPoolSizeLarge
	}
	lockers := make([]*locker, 0, size)
	for i := uint32(0); i < size; i++ {
		lockers = append(lockers, &locker{})
	}
	return &ScopeLocker{
		size:    size - 1,
		lockers: lockers,
	}
}

func (sl *ScopeLocker) LockWriteKey(khash uint32) func() {
	return sl.lockers[khash&sl.size].getWLock()
}

func (sl *ScopeLocker) LockReadKey(khash uint32) func() {
	return sl.lockers[khash&sl.size].getRLock()
}

func (sl *ScopeLocker) LockKey(khash uint32, cmd string) func() {
	return sl.lockers[khash&sl.size].lockKey(cmd)
}
