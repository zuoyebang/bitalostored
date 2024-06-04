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

package bytepools

import (
	"sync"
)

const (
	poolsNum       = 13
	maxPoolBufSize = 36 << 10
)

type BytePools struct {
	pools [poolsNum]sync.Pool
}

func NewBytePools() *BytePools {
	p := new(BytePools)
	for i := 0; i < poolsNum; i++ {
		size := 16 * (1 << i)
		if i == poolsNum-1 {
			size = maxPoolBufSize
		}
		p.pools[i] = sync.Pool{
			New: func() interface{} {
				return make([]byte, size)
			},
		}
	}
	return p
}

func (p *BytePools) getIndex(size int) int {
	if size <= 128 {
		switch {
		case size <= 16:
			return 0
		case size <= 32:
			return 1
		case size <= 64:
			return 2
		default:
			return 3
		}
	} else if size <= 2048 {
		switch {
		case size <= 256:
			return 4
		case size <= 512:
			return 5
		case size <= 1024:
			return 6
		default:
			return 7
		}
	} else {
		switch {
		case size <= 4096:
			return 8
		case size <= 8192:
			return 9
		case size <= 16384:
			return 10
		case size <= 32768:
			return 11
		case size <= maxPoolBufSize:
			return 12
		default:
			return -1
		}
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
