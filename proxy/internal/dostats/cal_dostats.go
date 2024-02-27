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

package dostats

import (
	"sync"
	"sync/atomic"
	"time"
)

type CalDoStats struct {
	mu sync.RWMutex

	total atomic.Int64
	opmap map[string]*OpStats
	flush struct {
		n uint
	}
}

func NewCalDoStats() *CalDoStats {
	return &CalDoStats{
		mu:    sync.RWMutex{},
		opmap: make(map[string]*OpStats, 32),
	}
}

func (s *CalDoStats) IncrOpTotal() {
	s.total.Add(1)
}

func (s *CalDoStats) getOpStats(opstr string) *OpStats {
	s.mu.RLock()
	e := s.opmap[opstr]
	s.mu.RUnlock()
	if e == nil {
		e = &OpStats{Opstr: opstr}
		s.mu.Lock()
		s.opmap[opstr] = e
		s.mu.Unlock()
	}
	return e
}

func (s *CalDoStats) IncrOpStats(opstr string, startUnixNano int64) {
	e := s.getOpStats(opstr)
	e.Calls.Add(1)
	e.Nsecs.Add(time.Now().UnixNano() - startUnixNano)
}

func (s *CalDoStats) IncrOpFails(opstr string, err error) error {
	e := s.getOpStats(opstr)
	e.Fails.Add(1)
	e.PeriodFails.Add(1)
	return err
}

func (s *CalDoStats) FlushOpStats(ct CmdType) {
	IncrOpTotal(ct, s.total.Swap(0))
	s.mu.RLock()
	for _, e := range s.opmap {
		if e.Calls.Load() != 0 {
			IncrOpStats(ct, e)
		}
	}
	s.mu.RUnlock()
	s.flush.n++

	if len(s.opmap) <= 32 {
		return
	}

	if (s.flush.n % 16384) == 0 {
		s.mu.Lock()
		s.opmap = make(map[string]*OpStats, 32)
		s.mu.Unlock()
	}
}
