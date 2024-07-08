// Copyright 2019-2022 The Zuoyebang-Stored and Zuoyebang-Bitalosdb Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found in the LICENSE file.

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
