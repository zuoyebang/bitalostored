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

package mmap

import (
	"encoding/binary"
	"errors"
	"os"
	"reflect"
	"unsafe"
)

const (
	// RDONLY maps the memory read-only.
	// Attempts to write to the MMap object will result in undefined behavior.
	RDONLY = 0
	// RDWR maps the memory as read-write. Writes to the MMap object will update the
	// underlying file.
	RDWR = 1 << iota
	// COPY maps the memory as copy-on-write. Writes to the MMap object will affect
	// memory, but the underlying file will remain unchanged.
	COPY
	// EXEC if set, the mapped memory is marked as executable.
	EXEC
)

const (
	ANON = 1 << iota
)

type MMap struct {
	name   string
	file   *os.File
	size   int64
	offset int64
	m      Mbuf
}

func Open(name string, size int64) (*MMap, error) {
	m := &MMap{
		name:   name,
		size:   size,
		offset: 0,
	}

	var err error

	m.file, err = os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	if m.size == 0 {
		st, _ := m.file.Stat()
		m.size = st.Size()
	}

	if err = m.file.Truncate(m.size); err != nil {
		return nil, err
	}

	if m.m, err = Map(m.file, RDWR, 0); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *MMap) ReadPointer(pos int) unsafe.Pointer {
	return unsafe.Pointer(&m.m[pos])
}

func (m *MMap) WriteInt64At(val int64, pos int) {
	binary.PutVarint(m.m[pos:pos+8], val)
}

func (m *MMap) ReadInt64At(pos int) int64 {
	res, _ := binary.Varint(m.m[pos : pos+8])
	return res
}

func (m *MMap) ReadUInt64At(pos int) uint64 {
	res, _ := binary.Uvarint(m.m[pos : pos+8])
	return res
}

func (m *MMap) WriteUInt64At(val uint64, pos int) {
	binary.PutUvarint(m.m[pos:pos+8], val)
}

func (m *MMap) ReadUInt16At(pos int) uint16 {
	return binary.LittleEndian.Uint16(m.m[pos : pos+2])
}

func (m *MMap) WriteUInt16At(val uint16, pos int) {
	binary.LittleEndian.PutUint16(m.m[pos:pos+2], val)
}

func (m *MMap) ReadUInt32At(pos int) uint32 {
	return binary.LittleEndian.Uint32(m.m[pos : pos+4])
}

func (m *MMap) WriteUInt32At(val uint32, pos int) {
	binary.LittleEndian.PutUint32(m.m[pos:pos+4], val)
}

func (m *MMap) ReadUInt8At(pos int) uint8 {
	return m.m[pos]
}

func (m *MMap) WriteUInt8At(val uint8, pos int) {
	m.m[pos] = val
}

func (m *MMap) GetMBuf() []byte {
	return m.m
}

func (m *MMap) Len() int {
	return len(m.m)
}

func (m *MMap) Cap() int {
	return cap(m.m)
}

func (m *MMap) WriteAt(val []byte, pos int) {
	size := len(val)
	copy(m.m[pos:pos+size], val)
}

func (m *MMap) ReadAt(pos, size int) []byte {
	return m.m[pos : pos+size : pos+size]
}

// Lock keeps the mapped region in physical memory, ensuring that it will not be
// swapped out.
func (m *MMap) Lock() error {
	return m.m.lock()
}

// Unlock reverses the effect of Lock, allowing the mapped region to potentially
// be swapped out. If m is already unlocked, aan error will result.
func (m *MMap) Unlock() error {
	return m.m.unlock()
}

func (m *MMap) Flush() (err error) {
	return m.m.flush()
}

func (m *MMap) Unmap() error {
	err := m.m.unmap()
	m.m = nil
	return err
}

func (m *MMap) Close() (err error) {
	if err = m.m.flush(); err != nil {
		return err
	}

	return m.file.Close()
}

type Mbuf []byte

func Map(f *os.File, prot, length int) (Mbuf, error) {
	return MapRegion(f, length, prot, 0, 0)
}

// MapRegion maps part of a file into memory.
// The offset parameter must be a multiple of the system's page size.
// If length < 0, the entire file will be mapped.
// If ANON is set in flags, f is ignored.
func MapRegion(f *os.File, length int, prot, flags int, offset int64) (Mbuf, error) {
	if offset%int64(os.Getpagesize()) != 0 {
		return nil, errors.New("offset parameter must be a multiple of the system's page size")
	}

	var fd uintptr
	if flags&ANON == 0 {
		fd = f.Fd()
		if length <= 0 {
			fi, err := f.Stat()
			if err != nil {
				return nil, err
			}
			length = int(fi.Size())
		}
	} else {
		if length <= 0 {
			return nil, errors.New("anonymous mapping requires non-zero length")
		}
		fd = ^uintptr(0)
	}

	return mmapfd(length, uintptr(prot), uintptr(flags), fd, offset)
}

func (m *Mbuf) header() *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(m))
}

func (m *Mbuf) addrLen() (uintptr, uintptr) {
	header := m.header()
	return header.Data, uintptr(header.Len)
}

func (m Mbuf) Flush() error {
	return m.flush()
}

func (m *Mbuf) Unmap() error {
	err := m.unmap()
	*m = nil
	return err
}
