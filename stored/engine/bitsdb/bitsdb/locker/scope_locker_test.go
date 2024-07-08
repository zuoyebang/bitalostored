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
	"testing"
	"time"

	"github.com/zuoyebang/bitalostored/butils/hash"
)

func TestScopeLocker(t *testing.T) {
	l := NewScopeLocker(true)
	key := []byte("a")
	khash := hash.Fnv32(key)
	unlockFunc := l.LockWriteKey(khash)
	go func() {
		unlockFunc := l.LockWriteKey(khash)
		defer unlockFunc()
	}()

	time.Sleep(5 * time.Second)
	unlockFunc()
	time.Sleep(time.Second)
}
