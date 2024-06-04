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

package map2

import (
	"arena"
	"encoding/binary"
	"sort"
)

const (
	SuccinctVersion      = 1
	SuccinctShardItemMax = 1024
	SuccinctHeaderSize   = 8
	SuccinctShardSize    = 8
	SuccinctItem32Size   = 8
	SuccinctItem64Size   = 12
)

type SuccinctMap struct {
	header Header
	size   uint32
	length uint32
	type32 bool
	data   []byte
	data32 []Item32Array
	data64 []Item64Array
	arena  *arena.Arena
}

type Header struct {
	version  uint16
	reserved uint16
	shards   uint32
}

type Shard struct {
	offset uint32
	length uint32
}

type Item32 struct {
	key   uint32
	value uint32
}

type Item64 struct {
	key   uint32
	value uint64
}

type Item32Array []Item32

func (i32 Item32Array) Len() int {
	return len(i32)
}

func (i32 Item32Array) Swap(i, j int) {
	i32[i], i32[j] = i32[j], i32[i]
}

func (i32 Item32Array) Less(i, j int) bool {
	return i32[i].key < i32[j].key
}

type Item64Array []Item64

func (i64 Item64Array) Len() int {
	return len(i64)
}

func (i64 Item64Array) Swap(i, j int) {
	i64[i], i64[j] = i64[j], i64[i]
}

func (i64 Item64Array) Less(i, j int) bool {
	return i64[i].key < i64[j].key
}

func NewSuccinctMap(type32 bool) *SuccinctMap {
	m := &SuccinctMap{
		header: Header{version: SuccinctVersion, reserved: 0, shards: 0},
		size:   0,
		length: 0,
		type32: type32,
		data:   nil,
		data32: nil,
		data64: nil,
		arena:  nil,
	}

	return m
}

func (s *SuccinctMap) Size() uint32 {
	return s.size
}

func (s *SuccinctMap) Length() uint32 {
	return s.length
}

func (s *SuccinctMap) GetData() []byte {
	return s.data
}

func (s *SuccinctMap) SetReader(d []byte) bool {
	if d == nil || len(d) <= SuccinctHeaderSize {
		return false
	}

	s.data = d
	s.header = s.readHeader(s.data)

	return true
}

func (s *SuccinctMap) InitWriter(count uint32) {
	shards := (count / SuccinctShardItemMax) + 1<<4

	s.header = Header{version: SuccinctVersion, reserved: 0, shards: shards}
	s.size = SuccinctHeaderSize + shards*SuccinctShardSize
	s.length = count
	s.data = nil
	s.arena = arena.NewArena()

	if s.type32 {
		s.data32 = arena.MakeSlice[Item32Array](s.arena, int(shards), int(shards))
	} else {
		s.data64 = arena.MakeSlice[Item64Array](s.arena, int(shards), int(shards))
	}
}

func (s *SuccinctMap) SetWriter(d []byte) bool {
	if d == nil || len(d) < int(s.size) || cap(d) < int(s.size) {
		return false
	}

	s.data = d

	return true
}

func (s *SuccinctMap) Store(key uint32, value any) {
	switch value.(type) {
	case uint32:
		if s.type32 {
			s.store32Internal(key, value.(uint32))
		}
		return
	case uint64:
		if !s.type32 {
			s.store64Internal(key, value.(uint64))
		}
		return
	default:
		return
	}
}

func (s *SuccinctMap) Add(key uint32, value any) {
	switch value.(type) {
	case uint32:
		if s.type32 {
			s.add32Internal(key, value.(uint32))
		}
		return
	case uint64:
		if !s.type32 {
			s.add64Internal(key, value.(uint64))
		}
		return
	default:
		return
	}
}

func (s *SuccinctMap) Serialize() bool {
	if s.type32 {
		return s.serialize32Internal()
	} else {
		return s.serialize64Internal()
	}
}

func (s *SuccinctMap) Load(key uint32) (any, bool) {
	if s.type32 {
		return s.load32Internal(key)
	} else {
		return s.load64Internal(key)
	}
}

func (s *SuccinctMap) Get(key uint32) (any, bool) {
	if s.type32 {
		return s.get32Internal(key)
	} else {
		return s.get64Internal(key)
	}
}

func (s *SuccinctMap) store32Internal(key uint32, value uint32) {
	if s.header.shards <= 0 {
		return
	}

	sid := key % s.header.shards

	if len(s.data32[sid]) == 0 {
		s.data32[sid] = arena.MakeSlice[Item32](s.arena, 0, SuccinctShardItemMax/2)
	}

	itemArray := &s.data32[sid]

	index := sort.Search(len(*itemArray),
		func(i int) bool {
			var ret int
			if (*itemArray)[i].key == key {
				ret = 0
			} else if (*itemArray)[i].key < key {
				ret = -1
			} else {
				ret = 1
			}
			return ret != -1
		},
	)

	exist := len(*itemArray) > 0 && index < len(*itemArray) && (*itemArray)[index].key == key
	if !exist {
		*itemArray = append(*itemArray, Item32{})
		copy((*itemArray)[index+1:], (*itemArray)[index:])
	}

	item := &(*itemArray)[index]
	item.key = key
	item.value = value

	s.size += SuccinctItem32Size
}

func (s *SuccinctMap) store64Internal(key uint32, value uint64) {
	if s.header.shards <= 0 {
		return
	}

	sid := key % s.header.shards

	if len(s.data64[sid]) == 0 {
		s.data64[sid] = arena.MakeSlice[Item64](s.arena, 0, SuccinctShardItemMax/2)
	}

	itemArray := &s.data64[sid]

	index := sort.Search(len(*itemArray),
		func(i int) bool {
			var ret int
			if (*itemArray)[i].key == key {
				ret = 0
			} else if (*itemArray)[i].key < key {
				ret = -1
			} else {
				ret = 1
			}
			return ret != -1
		},
	)

	exist := len(*itemArray) > 0 && index < len(*itemArray) && (*itemArray)[index].key == key
	if !exist {
		*itemArray = append(*itemArray, Item64{})
		copy((*itemArray)[index+1:], (*itemArray)[index:])
	}

	item := &(*itemArray)[index]
	item.key = key
	item.value = value

	s.size += SuccinctItem64Size
}

func (s *SuccinctMap) add32Internal(key uint32, value uint32) {
	if s.header.shards <= 0 {
		return
	}

	sid := key % s.header.shards

	if len(s.data32[sid]) == 0 {
		s.data32[sid] = arena.MakeSlice[Item32](s.arena, 0, SuccinctShardItemMax)
	}

	s.data32[sid] = append(s.data32[sid], Item32{key: key, value: value})

	s.size += SuccinctItem32Size
}

func (s *SuccinctMap) add64Internal(key uint32, value uint64) {
	if s.header.shards <= 0 {
		return
	}

	sid := key % s.header.shards

	if len(s.data64[sid]) == 0 {
		s.data64[sid] = arena.MakeSlice[Item64](s.arena, 0, SuccinctShardItemMax)
	}

	s.data64[sid] = append(s.data64[sid], Item64{key: key, value: value})

	s.size += SuccinctItem64Size
}

func (s *SuccinctMap) serialize32Internal() bool {
	if s.size <= SuccinctHeaderSize || s.length <= 0 || len(s.data32) <= 0 {
		return false
	}

	shardOffset := uint32(0)
	itemOffset := SuccinctHeaderSize + s.header.shards*SuccinctShardSize

	if s.data == nil {
		s.data = arena.MakeSlice[byte](s.arena, int(s.size), int(s.size))
	}

	s.writeHeader(s.data[shardOffset:], s.header)
	shardOffset += SuccinctHeaderSize

	for i := uint32(0); i < s.header.shards; i++ {
		itemsLen := uint32(len(s.data32[i]))
		s.writeShard(s.data[shardOffset:], Shard{offset: itemOffset, length: itemsLen})
		shardOffset += SuccinctShardSize

		if itemsLen <= 0 {
			continue
		}

		sort.Sort(s.data32[i])
		for j := uint32(0); j < itemsLen; j++ {
			s.writeItem32(s.data[itemOffset:], s.data32[i][j])
			itemOffset += SuccinctItem32Size
		}
	}

	return true
}

func (s *SuccinctMap) serialize64Internal() bool {
	if s.size <= SuccinctHeaderSize || s.length <= 0 || len(s.data64) <= 0 {
		return false
	}

	shardOffset := uint32(0)
	itemOffset := SuccinctHeaderSize + s.header.shards*SuccinctShardSize

	if s.data == nil {
		s.data = arena.MakeSlice[byte](s.arena, int(s.size), int(s.size))
	}

	s.writeHeader(s.data[shardOffset:], s.header)
	shardOffset += SuccinctHeaderSize

	for i := uint32(0); i < s.header.shards; i++ {
		itemsLen := uint32(len(s.data64[i]))
		s.writeShard(s.data[shardOffset:], Shard{offset: itemOffset, length: itemsLen})
		shardOffset += SuccinctShardSize

		if itemsLen <= 0 {
			continue
		}

		sort.Sort(s.data64[i])
		for j := uint32(0); j < itemsLen; j++ {
			s.writeItem64(s.data[itemOffset:], s.data64[i][j])
			itemOffset += SuccinctItem64Size
		}
	}

	return true
}

func (s *SuccinctMap) load32Internal(key uint32) (uint32, bool) {
	if len(s.data32) < int(s.header.shards) || s.header.shards <= 0 {
		return 0, false
	}

	sid := key % s.header.shards

	if len(s.data32[sid]) == 0 {
		return 0, false
	}

	itemArray := &s.data32[sid]

	ok, idx := s.findItem32Arr(key, *itemArray, len(*itemArray))
	if !ok {
		return 0, false
	}

	return (*itemArray)[idx].value, true
}

func (s *SuccinctMap) load64Internal(key uint32) (uint64, bool) {
	if len(s.data64) < int(s.header.shards) || s.header.shards <= 0 {
		return 0, false
	}

	sid := key % s.header.shards

	if len(s.data64[sid]) == 0 {
		return 0, false
	}

	itemArray := &s.data64[sid]

	ok, idx := s.findItem64Arr(key, *itemArray, len(*itemArray))
	if !ok {
		return 0, false
	}

	return (*itemArray)[idx].value, true
}

func (s *SuccinctMap) get32Internal(key uint32) (uint32, bool) {
	if len(s.data) <= SuccinctHeaderSize || s.header.shards <= 0 {
		return 0, false
	}

	sid := key % s.header.shards
	curOffset := SuccinctHeaderSize + sid*SuccinctShardSize

	shard := s.readShard(s.data[curOffset:])
	if shard.length <= 0 {
		return 0, false
	}

	curOffset = shard.offset

	ok, idx := s.findItem32(key, s.data[curOffset:], int(shard.length))
	if !ok {
		return 0, false
	}

	curOffset += uint32(idx * SuccinctItem32Size)
	item32 := s.readItem32(s.data[curOffset:])

	return item32.value, true
}

func (s *SuccinctMap) get64Internal(key uint32) (uint64, bool) {
	if len(s.data) <= SuccinctHeaderSize || s.header.shards <= 0 {
		return 0, false
	}

	sid := key % s.header.shards
	curOffset := SuccinctHeaderSize + sid*SuccinctShardSize

	shard := s.readShard(s.data[curOffset:])
	if shard.length <= 0 {
		return 0, false
	}

	curOffset = shard.offset

	ok, idx := s.findItem64(key, s.data[curOffset:], int(shard.length))
	if !ok {
		return 0, false
	}

	curOffset += uint32(idx * SuccinctItem64Size)
	item64 := s.readItem64(s.data[curOffset:])

	return item64.value, true
}

func (s *SuccinctMap) Finish() {
	s.size = SuccinctHeaderSize
	s.length = 0
	s.data32 = nil
	s.data64 = nil
	if s.arena != nil {
		s.arena.Free()
		s.arena = nil
	}
}

func (s *SuccinctMap) writeHeader(buf []byte, header Header) {
	binary.BigEndian.PutUint16(buf[0:], header.version)
	binary.BigEndian.PutUint16(buf[2:], header.reserved)
	binary.BigEndian.PutUint32(buf[4:], header.shards)
}

func (s *SuccinctMap) writeShard(buf []byte, shard Shard) {
	binary.BigEndian.PutUint32(buf[0:], shard.offset)
	binary.BigEndian.PutUint32(buf[4:], shard.length)
}

func (s *SuccinctMap) writeItem32(buf []byte, item32 Item32) {
	binary.BigEndian.PutUint32(buf[0:], item32.key)
	binary.BigEndian.PutUint32(buf[4:], item32.value)
}

func (s *SuccinctMap) writeItem64(buf []byte, item64 Item64) {
	binary.BigEndian.PutUint32(buf[0:], item64.key)
	binary.BigEndian.PutUint64(buf[4:], item64.value)
}

func (s *SuccinctMap) readHeader(buf []byte) Header {
	header := Header{
		version:  binary.BigEndian.Uint16(buf[0:]),
		reserved: binary.BigEndian.Uint16(buf[2:]),
		shards:   binary.BigEndian.Uint32(buf[4:]),
	}

	return header
}

func (s *SuccinctMap) readShard(buf []byte) Shard {
	shard := Shard{
		offset: binary.BigEndian.Uint32(buf[0:]),
		length: binary.BigEndian.Uint32(buf[4:]),
	}

	return shard
}

func (s *SuccinctMap) readItem32(buf []byte) Item32 {
	item32 := Item32{
		key:   binary.BigEndian.Uint32(buf[0:]),
		value: binary.BigEndian.Uint32(buf[4:]),
	}

	return item32
}

func (s *SuccinctMap) readItem64(buf []byte) Item64 {
	item64 := Item64{
		key:   binary.BigEndian.Uint32(buf[0:]),
		value: binary.BigEndian.Uint64(buf[4:]),
	}

	return item64
}

func (s *SuccinctMap) findItem32(key uint32, buf []byte, n int) (bool, int) {
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1)
		if binary.BigEndian.Uint32(buf[SuccinctItem32Size*h:]) < key {
			i = h + 1
		} else {
			j = h
		}
	}

	if i < n && binary.BigEndian.Uint32(buf[SuccinctItem32Size*i:]) == key {
		return true, i
	}

	return false, 0
}

func (s *SuccinctMap) findItem32Arr(key uint32, arr Item32Array, n int) (bool, int) {
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1)
		if arr[h].key < key {
			i = h + 1
		} else {
			j = h
		}
	}

	if i < n && arr[i].key == key {
		return true, i
	}

	return false, 0
}

func (s *SuccinctMap) findItem64Arr(key uint32, arr Item64Array, n int) (bool, int) {
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1)
		if arr[h].key < key {
			i = h + 1
		} else {
			j = h
		}
	}

	if i < n && arr[i].key == key {
		return true, i
	}

	return false, 0
}

func (s *SuccinctMap) findItem64(key uint32, buf []byte, n int) (bool, int) {
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1)
		if binary.BigEndian.Uint32(buf[SuccinctItem64Size*h:]) < key {
			i = h + 1
		} else {
			j = h
		}
	}

	if i < n && binary.BigEndian.Uint32(buf[SuccinctItem64Size*i:]) == key {
		return true, i
	}

	return false, 0
}
