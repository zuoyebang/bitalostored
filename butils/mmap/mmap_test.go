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
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

const testMmapFile = "hi.mmap"

func TestMMap(t *testing.T) {
	t.Log(os.Getpid())
	m, e := Open(testMmapFile, 32)
	if e != nil {
		t.Error(e)
	}

	n := m.ReadInt64At(0)
	t.Log("read", n)

	n += 20
	m.WriteInt64At(n, 0)
	t.Log("read", m.ReadInt64At(0))

	n = m.ReadInt64At(8)
	t.Log("read", n)

	n += 20
	m.WriteInt64At(n, 8)
	t.Log("read", m.ReadInt64At(8))

	n = m.ReadInt64At(16)
	t.Log("read", n)

	n += 20
	m.WriteInt64At(n, 16)
	t.Log("read", m.ReadInt64At(16))

	v := []byte("abcdefgh")
	m.WriteAt(v, 24)
	wv := m.ReadAt(24, 8)
	t.Log("read", string(wv))
	os.Remove(testMmapFile)
}

func TestReadMmap(t *testing.T) {
	m, e := Open(testMmapFile, 24)
	if e != nil {
		t.Error(e)
	}
	n := m.ReadInt64At(0)
	t.Log("read", n)
	os.Remove(testMmapFile)
}

var testData = []byte("0123456789ABCDEF")
var testPath = filepath.Join(os.TempDir(), "testdata")

func init() {
	f := openFile(os.O_RDWR | os.O_CREATE | os.O_TRUNC)
	f.Write(testData)
	f.Close()
}

func openFile(flags int) *os.File {
	f, err := os.OpenFile(testPath, flags, 0644)
	if err != nil {
		panic(err.Error())
	}
	return f
}

func TestUnmap(t *testing.T) {
	f := openFile(os.O_RDONLY)
	defer f.Close()
	m, err := Map(f, RDONLY, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
	}
	if err := m.Unmap(); err != nil {
		t.Errorf("error unmapping: %s", err)
	}
}

func TestReadWrite(t *testing.T) {
	f := openFile(os.O_RDWR)
	defer f.Close()
	m, err := Map(f, RDWR, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
	}
	defer m.Unmap()
	if !bytes.Equal(testData, m) {
		t.Errorf("mmap != testData: %q, %q", m, testData)
	}

	m[9] = 'X'
	m.Flush()

	fileData, err := ioutil.ReadAll(f)
	if err != nil {
		t.Errorf("error reading file: %s", err)
	}
	if !bytes.Equal(fileData, []byte("012345678XABCDEF")) {
		t.Errorf("file wasn't modified")
	}

	// leave things how we found them
	m[9] = '9'
	m.Flush()
}

func TestProtFlagsAndErr(t *testing.T) {
	f := openFile(os.O_RDONLY)
	defer f.Close()
	if _, err := Map(f, RDWR, 0); err == nil {
		t.Errorf("expected error")
	}
}

func TestFlags(t *testing.T) {
	f := openFile(os.O_RDWR)
	defer f.Close()
	m, err := Map(f, COPY, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
	}
	defer m.Unmap()

	m[9] = 'X'
	m.Flush()

	fileData, err := ioutil.ReadAll(f)
	if err != nil {
		t.Errorf("error reading file: %s", err)
	}
	if !bytes.Equal(fileData, testData) {
		t.Errorf("file was modified")
	}
}

// Test that we can map files from non-0 offsets
// The page size on most Unixes is 4KB, but on Windows it's 64KB
func TestNonZeroOffset(t *testing.T) {
	const pageSize = 65536

	// Create a 2-page sized file
	bigFilePath := filepath.Join(os.TempDir(), "nonzero")
	fileobj, err := os.OpenFile(bigFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err.Error())
	}

	bigData := make([]byte, 2*pageSize, 2*pageSize)
	fileobj.Write(bigData)
	fileobj.Close()

	// Map the first page by itself
	fileobj, err = os.OpenFile(bigFilePath, os.O_RDONLY, 0)
	if err != nil {
		panic(err.Error())
	}
	m, err := MapRegion(fileobj, pageSize, RDONLY, 0, 0)
	if err != nil {
		t.Errorf("error mapping file: %s", err)
	}
	m.Unmap()
	fileobj.Close()

	// Map the second page by itself
	fileobj, err = os.OpenFile(bigFilePath, os.O_RDONLY, 0)
	if err != nil {
		panic(err.Error())
	}
	m, err = MapRegion(fileobj, pageSize, RDONLY, 0, pageSize)
	if err != nil {
		t.Errorf("error mapping file: %s", err)
	}
	err = m.Unmap()
	if err != nil {
		t.Error(err)
	}

	m, err = MapRegion(fileobj, pageSize, RDONLY, 0, 1)
	if err == nil {
		t.Error("expect error because offset is not multiple of page size")
	}

	fileobj.Close()
}

func TestAnonymousMapping(t *testing.T) {
	const size = 4 * 1024

	// Make an anonymous region
	mem, err := MapRegion(nil, size, RDWR, ANON, 0)
	if err != nil {
		t.Fatalf("failed to allocate memory for buffer: %v", err)
	}

	// Check memory writable
	for i := 0; i < size; i++ {
		mem[i] = 0x55
	}

	// And unmap it
	err = mem.Unmap()
	if err != nil {
		t.Fatalf("failed to unmap memory for buffer: %v", err)
	}
}
