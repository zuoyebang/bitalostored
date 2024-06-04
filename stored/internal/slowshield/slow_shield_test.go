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

package slowshield

import (
	"testing"
	"time"
)

func TestNewSlowControl(t *testing.T) {
	sc := NewSlowShield()
	cmd1 := "zrange"
	cmd2 := "zcard"
	key1 := []byte("aa")
	key2 := []byte("bbb")
	for i := 0; i <= 1000000; i++ {
		sc.Send(cmd1, key1, 100000000000*time.Millisecond.Nanoseconds())
		sc.Send(cmd1, key2, 200000000000*time.Millisecond.Nanoseconds())
	}

	time.Sleep(1 * time.Second)
	if sc.CheckSlowShield(cmd1, key1) {
		t.Log("hit slow shield : ", string(cmd1), string(key1))
	}
	if sc.CheckSlowShield(cmd1, key2) {
		t.Log("hit slow shield : ", string(cmd1), string(key2))
	}
	if !sc.CheckSlowShield(cmd2, key1) {
		t.Log("not hit slow shield : ", string(cmd2), string(key1))
	}
	if !sc.CheckSlowShield(cmd2, key2) {
		t.Log("not hit slow shield : ", string(cmd2), string(key2))
	}
}
