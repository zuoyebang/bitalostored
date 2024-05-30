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

package list2

import "testing"

func TestStack(t *testing.T) {
	stack := NewStack()
	stack.Push(1)
	stack.Push(2)
	stack.Push(3)
	stack.Push(4)

	length := stack.Len()
	if length != 4 {
		t.Errorf("stack.Len() failed. Got %d, expected 4.", length)
	}

	value := stack.Peak().(int)
	if value != 4 {
		t.Errorf("stack.Peak() failed. Got %d, expected 4.", value)
	}

	value = stack.Pop().(int)
	if value != 4 {
		t.Errorf("stack.Pop() failed. Got %d, expected 4.", value)
	}

	length = stack.Len()
	if length != 3 {
		t.Errorf("stack.Len() failed. Got %d, expected 3.", length)
	}

	value = stack.Peak().(int)
	if value != 3 {
		t.Errorf("stack.Peak() failed. Got %d, expected 3.", value)
	}

	value = stack.Pop().(int)
	if value != 3 {
		t.Errorf("stack.Pop() failed. Got %d, expected 3.", value)
	}

	value = stack.Pop().(int)
	if value != 2 {
		t.Errorf("stack.Pop() failed. Got %d, expected 2.", value)
	}

	empty := stack.Empty()
	if empty {
		t.Errorf("stack.Empty() failed. Got %v, expected false.", empty)
	}

	value = stack.Pop().(int)
	if value != 1 {
		t.Errorf("stack.Pop() failed. Got %d, expected 1.", value)
	}

	empty = stack.Empty()
	if !empty {
		t.Errorf("stack.Empty() failed. Got %v, expected true.", empty)
	}

	nilValue := stack.Peak()
	if nilValue != nil {
		t.Errorf("stack.Peak() failed. Got %d, expected nil.", nilValue)
	}

	nilValue = stack.Pop()
	if nilValue != nil {
		t.Errorf("stack.Pop() failed. Got %d, expected nil.", nilValue)
	}

	length = stack.Len()
	if length != 0 {
		t.Errorf("stack.Len() failed. Got %d, expected 0.", length)
	}
}
