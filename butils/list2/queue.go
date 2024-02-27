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
	"container/list"
	"math/bits"
)

type Queue struct {
	list *list.List
}

func NewQueue() *Queue {
	l := list.New()
	return &Queue{l}
}

func (q *Queue) Push(value interface{}) {
	q.list.PushFront(value)
}

func (q *Queue) Pop() interface{} {
	e := q.list.Back()
	if e != nil {
		q.list.Remove(e)
		return e.Value
	}
	return nil
}

func (q *Queue) Peak() interface{} {
	e := q.list.Back()
	if e != nil {
		return e.Value
	}

	return nil
}

func (q *Queue) Len() int {
	return q.list.Len()
}

func (q *Queue) Empty() bool {
	return q.list.Len() == 0
}

const (
	dequeueBits = 32
	mask        = 1<<dequeueBits - 1
)

type IntQueue struct {
	headTail uint64
	data     []int32
}

func NewIntQueue(size uint32) *IntQueue {
	sq := &IntQueue{headTail: 0}
	sq.data = make([]int32, calcBitsSize(int(size)))

	return sq
}

func (q *IntQueue) unpack(ptrs uint64) (head, tail uint32) {
	head = uint32((ptrs >> dequeueBits) & mask)
	tail = uint32(ptrs & mask)
	return
}

func (q *IntQueue) pack(head, tail uint32) uint64 {
	return (uint64(head) << dequeueBits) | uint64(tail&mask)
}

func (q *IntQueue) Push(value int32) {
	head, _ := q.unpack(q.headTail)
	q.data[head&uint32(len(q.data)-1)] = value
	q.headTail += 1 << dequeueBits
}

func (q *IntQueue) Front() (int32, bool) {
	head, tail := q.unpack(q.headTail)
	if tail == head {
		return 0, false
	}

	value := q.data[tail&uint32(len(q.data)-1)]

	return value, true
}

func (q *IntQueue) Pop() (int32, bool) {
	head, tail := q.unpack(q.headTail)
	if tail == head {
		return 0, false
	}

	index := tail & uint32(len(q.data)-1)
	value := q.data[index]
	q.data[index] = 0

	q.headTail = q.pack(head, tail+1)
	return value, true
}

func (q *IntQueue) Empty() bool {
	head, tail := q.unpack(q.headTail)
	return tail == head
}

func (q *IntQueue) Len() int {
	head, tail := q.unpack(q.headTail)
	return int(head) - int(tail)
}

type Int64Queue struct {
	headTail uint64
	data     []int64
}

func NewInt64Queue(size uint32) *Int64Queue {
	sq := &Int64Queue{headTail: 0}
	sq.data = make([]int64, calcBitsSize(int(size)))

	return sq
}

func (q *Int64Queue) unpack(ptrs uint64) (head, tail uint32) {
	head = uint32((ptrs >> dequeueBits) & mask)
	tail = uint32(ptrs & mask)
	return
}

func (q *Int64Queue) pack(head, tail uint32) uint64 {
	return (uint64(head) << dequeueBits) | uint64(tail&mask)
}

func (q *Int64Queue) Push(value int64) {
	head, _ := q.unpack(q.headTail)
	q.data[head&uint32(len(q.data)-1)] = value
	q.headTail += 1 << dequeueBits
}

func (q *Int64Queue) Front() (int64, bool) {
	head, tail := q.unpack(q.headTail)
	if tail == head {
		return 0, false
	}

	value := q.data[tail&uint32(len(q.data)-1)]

	return value, true
}

func (q *Int64Queue) Pop() (int64, bool) {
	head, tail := q.unpack(q.headTail)
	if tail == head {
		return 0, false
	}

	index := tail & uint32(len(q.data)-1)
	value := q.data[index]
	q.data[index] = 0

	q.headTail = q.pack(head, tail+1)
	return value, true
}

func (q *Int64Queue) Empty() bool {
	head, tail := q.unpack(q.headTail)
	return tail == head
}

func (q *Int64Queue) Len() int {
	head, tail := q.unpack(q.headTail)
	return int(head) - int(tail)
}

func calcBitsSize(sz int) int {
	return 1 << bits.Len(uint(sz))
}
