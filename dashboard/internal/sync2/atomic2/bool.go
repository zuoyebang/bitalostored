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

package atomic2

type Bool struct {
	c Int64
}

func (b *Bool) Bool() bool {
	return b.IsTrue()
}

func (b *Bool) IsTrue() bool {
	return b.c.Int64() != 0
}

func (b *Bool) IsFalse() bool {
	return b.c.Int64() == 0
}

func (b *Bool) toInt64(v bool) int64 {
	if v {
		return 1
	} else {
		return 0
	}
}

func (b *Bool) Set(v bool) {
	b.c.Set(b.toInt64(v))
}

func (b *Bool) CompareAndSwap(o, n bool) bool {
	return b.c.CompareAndSwap(b.toInt64(o), b.toInt64(n))
}

func (b *Bool) Swap(v bool) bool {
	return b.c.Swap(b.toInt64(v)) != 0
}
