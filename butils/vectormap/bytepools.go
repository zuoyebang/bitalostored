// Copyright 2019 The Bitalos-Stored author and other contributors.
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

package vectormap

import (
	"sync"
)

const (
	poolsNum       = 4
	maxPoolBufSize = 128
)

var VMBytePools = NewBytePools()

type BytePools struct {
	pools [poolsNum]sync.Pool
}

func NewBytePools() *BytePools {
	p := new(BytePools)
	for i := 0; i < poolsNum; i++ {
		size := 16 * (1 << i)
		p.pools[i] = sync.Pool{
			New: func() interface{} {
				return make([]byte, size)
			},
		}
	}
	return p
}

func (p *BytePools) getIndex(size int) int {
	switch {
	case size <= 16:
		return 0
	case size <= 32:
		return 1
	case size <= 64:
		return 2
	case size <= 128:
		return 3
	default:
		return -1
	}
}

func (p *BytePools) Get(size int) interface{} {
	index := p.getIndex(size)
	if index == -1 {
		return make([]byte, size)
	}
	return p.pools[index].Get()
}

func (p *BytePools) Put(x []byte) {
	index := p.getIndex(len(x))
	if index >= 0 {
		p.pools[index].Put(x)
	}
}

func (p *BytePools) GetBytePool(size int) ([]byte, func()) {
	v := p.Get(size).([]byte)
	return v, func() {
		p.PutBytePool(v)
	}
}

func (p *BytePools) GetMaxBytePool() ([]byte, func()) {
	v := p.pools[poolsNum-1].Get().([]byte)
	return v, func() {
		p.pools[poolsNum-1].Put(v)
	}
}

func (p *BytePools) MakeValue(v []byte) ([]byte, func()) {
	size := len(v)
	pool := p.Get(size).([]byte)
	copy(pool[:size], v)
	return pool[:size], func() {
		p.PutBytePool(pool)
	}
}

func (p *BytePools) PutBytePool(v []byte) {
	if len(v) > maxPoolBufSize {
		return
	}
	p.Put(v)
}
