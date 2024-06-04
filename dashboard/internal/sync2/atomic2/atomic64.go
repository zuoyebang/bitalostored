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

package atomic2

import "sync/atomic"

type Int64 struct {
	v int64
}

func (a *Int64) Int64() int64 {
	return atomic.LoadInt64(&a.v)
}

func (a *Int64) AsInt() int {
	return int(a.Int64())
}

func (a *Int64) Set(v int64) {
	atomic.StoreInt64(&a.v, v)
}

func (a *Int64) CompareAndSwap(o, n int64) bool {
	return atomic.CompareAndSwapInt64(&a.v, o, n)
}

func (a *Int64) Swap(v int64) int64 {
	return atomic.SwapInt64(&a.v, v)
}

func (a *Int64) Add(v int64) int64 {
	return atomic.AddInt64(&a.v, v)
}

func (a *Int64) Sub(v int64) int64 {
	return a.Add(-v)
}

func (a *Int64) Incr() int64 {
	return a.Add(1)
}

func (a *Int64) Decr() int64 {
	return a.Add(-1)
}
