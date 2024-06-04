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

package router

import (
	"errors"
	"sync"
	"time"

	"github.com/zuoyebang/bitalostored/proxy/internal/config"
	"github.com/zuoyebang/bitalostored/proxy/internal/log"

	"github.com/sony/gobreaker"
)

const MinBreakerOpenRequest uint32 = 10

type GroupBreaker struct {
	mu    sync.RWMutex
	gbmap map[int]*Breaker
	opt   *BreakerOption
}

func NewGroupBreaker(conf *config.Config) *GroupBreaker {
	opt := &BreakerOption{}
	opt.BreakerOpenFailRate = conf.BreakerOpenFailRate
	opt.BreakerRestoreRequest = conf.BreakerRestoreRequest
	opt.BreakerStopTimeout = conf.BreakerStopTimeout.Duration()

	gb := &GroupBreaker{
		mu:    sync.RWMutex{},
		gbmap: make(map[int]*Breaker),
		opt:   opt,
	}
	return gb
}

func (gb *GroupBreaker) GetCircuitBreakerByGid(gid int) (*Breaker, error) {
	gb.mu.RLock()
	defer gb.mu.RUnlock()

	b, ok := gb.gbmap[gid]
	if !ok {
		return nil, errors.New("no gid breaker")
	}

	return b, nil
}

func (gb *GroupBreaker) AddCircuitBreaker(gid int, addrs ...string) {
	if len(addrs) <= 0 {
		return
	}

	gb.mu.Lock()
	defer gb.mu.Unlock()

	var b *Breaker
	if _, ok := gb.gbmap[gid]; !ok {
		b = NewCircuitBreaker(gb.opt)
		gb.gbmap[gid] = b
	} else {
		b = gb.gbmap[gid]
	}

	num := b.AddBreakerByAddrs(addrs...)
	if num > 0 {
		log.Infof("add breaker gid:%d addrs:%v num:%d", gid, addrs, num)
	}
}

func (gb *GroupBreaker) RemoveCircuitBreaker(gid int, addr string) {
	gb.mu.Lock()
	defer gb.mu.Unlock()

	b, ok := gb.gbmap[gid]
	if !ok {
		return
	}

	num := b.RemoveCircuitBreakerByAddr(addr)
	if num > 0 {
		log.Infof("remove breaker gid:%d addr:%s num:%d", gid, addr, num)
	}
}

type BreakerOption struct {
	BreakerStopTimeout    time.Duration
	BreakerOpenFailRate   float64
	BreakerRestoreRequest int
}

type Breaker struct {
	mu   sync.RWMutex
	bmap map[string]*gobreaker.CircuitBreaker
	opt  *BreakerOption
}

func NewCircuitBreaker(opt *BreakerOption) *Breaker {
	cgb := &Breaker{
		mu:   sync.RWMutex{},
		bmap: make(map[string]*gobreaker.CircuitBreaker),
		opt:  opt,
	}
	return cgb
}

func (b *Breaker) AddBreakerByAddrs(addrs ...string) int {
	if len(addrs) <= 0 {
		return 0
	}

	opts := gobreaker.Settings{
		Name:        "DEFAULT",
		Timeout:     b.opt.BreakerStopTimeout,
		MaxRequests: uint32(b.opt.BreakerRestoreRequest),
		Interval:    time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			if counts.Requests <= MinBreakerOpenRequest {
				return false
			}
			failureRate := float64(counts.TotalFailures) / float64(counts.Requests)
			ok := failureRate > b.opt.BreakerOpenFailRate
			if ok {
				log.Infof("braker open total requests:%d totalfailures:%d failrate:%f>%f",
					counts.Requests,
					counts.TotalFailures,
					failureRate,
					b.opt.BreakerOpenFailRate)
			}
			return ok
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			log.Infof("name:%s gobreaker State from %v to %v", name, from.String(), to.String())
		},
	}
	num := 0
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, addr := range addrs {
		opts.Name = addr
		if _, ok := b.bmap[addr]; !ok {
			b.bmap[addr] = gobreaker.NewCircuitBreaker(opts)
			num++
		}
	}
	return num
}

func (b *Breaker) RemoveCircuitBreakerByAddr(addrs ...string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	num := 0
	for _, addr := range addrs {
		if _, ok := b.bmap[addr]; ok {
			delete(b.bmap, addr)
			num++
		}
	}
	return num
}

func (b *Breaker) GetCircuitBreaker(hystrixName string) *gobreaker.CircuitBreaker {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.bmap[hystrixName]
}
