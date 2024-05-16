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
	"fmt"
	"sync/atomic"
)

type refcnt int32

func (v *refcnt) init(val int32) {
	*v = refcnt(val)
}

func (v *refcnt) refs() int32 {
	return atomic.LoadInt32((*int32)(v))
}

func (v *refcnt) acquire() {
	switch v := atomic.AddInt32((*int32)(v), 1); {
	case v <= 1:
		panic(fmt.Sprintf("cache: inconsistent reference count: %d", v))
	}
}

func (v *refcnt) release() bool {
	switch v := atomic.AddInt32((*int32)(v), -1); {
	case v < 0:
		panic(fmt.Sprintf("cache: inconsistent reference count: %d", v))
	case v == 0:
		return true
	default:
		return false
	}
}
