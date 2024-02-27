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

package list2

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestQueue(t *testing.T) {
	queue := NewQueue()
	queue.Push(1)
	queue.Push(2)
	queue.Push(3)
	queue.Push(4)

	length := queue.Len()
	if length != 4 {
		t.Errorf("queue.Len() failed. Got %d, expected 4.", length)
	}

	value := queue.Peak().(int)
	if value != 1 {
		t.Errorf("queue.Peak() failed. Got %d, expected 1.", value)
	}

	value = queue.Pop().(int)
	if value != 1 {
		t.Errorf("queue.Pop() failed. Got %d, expected 1.", value)
	}

	length = queue.Len()
	if length != 3 {
		t.Errorf("queue.Len() failed. Got %d, expected 3.", length)
	}

	value = queue.Peak().(int)
	if value != 2 {
		t.Errorf("queue.Peak() failed. Got %d, expected 2.", value)
	}

	value = queue.Pop().(int)
	if value != 2 {
		t.Errorf("queue.Pop() failed. Got %d, expected 2.", value)
	}

	value = queue.Pop().(int)
	if value != 3 {
		t.Errorf("queue.Pop() failed. Got %d, expected 3.", value)
	}

	empty := queue.Empty()
	if empty {
		t.Errorf("queue.Empty() failed. Got %v, expected false.", empty)
	}

	value = queue.Pop().(int)
	if value != 4 {
		t.Errorf("queue.Pop() failed. Got %d, expected 4.", value)
	}

	empty = queue.Empty()
	if !empty {
		t.Errorf("queue.Empty() failed. Got %v, expected true.", empty)
	}

	nilValue := queue.Peak()
	if nilValue != nil {
		t.Errorf("queue.Peak() failed. Got %d, expected nil.", nilValue)
	}

	nilValue = queue.Pop()
	if nilValue != nil {
		t.Errorf("queue.Pop() failed. Got %d, expected nil.", nilValue)
	}

	length = queue.Len()
	if length != 0 {
		t.Errorf("queue.Len() failed. Got %d, expected 0.", length)
	}
}

func TestIntQueue(t *testing.T) {
	size := 2 << 20
	queue := NewIntQueue(uint32(size))
	for i := 0; i < size; i++ {
		queue.Push(int32(i))
	}
	if queue.Empty() {
		t.Errorf("queue not empty")
	}

	succ := 0
	cursor := 0
	for {
		if queue.Len() != size-cursor {
			t.Errorf("queue len error. expect:%d actual:%d", uint32(size-cursor), queue.Len())
			break
		}

		if val, ok := queue.Pop(); !ok {
			break
		} else if val == int32(cursor) {
			succ++
			cursor++
		}
	}
	if succ != size {
		t.Errorf("queue pop failed. succ:%d, expected %d", succ, size)
	}
	if !queue.Empty() {
		t.Errorf("queue should be empty")
	}
}

func TestIntQueue2(t *testing.T) {
	size := 1 << 20
	total := 1<<32 + 1<<20
	queue := NewIntQueue(uint32(size))
	for i := 0; i < total; i++ {
		queue.Push(int32(i % (1 << 30)))
		if val, ok := queue.Pop(); !ok {
			t.Errorf("queue pop failed")
		} else if val != int32(i%(1<<30)) {
			t.Errorf("queue pop value error")
		}
	}
	if !queue.Empty() {
		t.Errorf("queue should be empty")
	}
}

func TestParallelWrite(t *testing.T) {
	size := 1 << 20
	queue := NewIntQueue(uint32(size))

	loop := 100
	opCount := 10000
	appearCount := loop
	var wg sync.WaitGroup
	var locker sync.RWMutex
	for i := 0; i < loop; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opCount; j++ {
				locker.Lock()
				queue.Push(int32(j))
				locker.Unlock()
			}
		}()
	}
	wg.Wait()

	valueCount := make([]int32, opCount)
	for i := 0; i < loop; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opCount; j++ {
				locker.Lock()
				val, _ := queue.Pop()
				locker.Unlock()
				atomic.AddInt32(&valueCount[int(val)], 1)
			}
		}()
	}
	wg.Wait()
	for i := 0; i < opCount; i++ {
		if valueCount[i] != int32(appearCount) {
			t.Errorf("value:%d count(%d)!=%d", i, valueCount[i], appearCount)
		}
	}
}
