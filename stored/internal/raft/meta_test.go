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

package raft

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRafRaftInit(t *testing.T) {
	dir := "/tmp/raftmeta"
	os.MkdirAll(dir, 0755)
	defer func() {
		os.RemoveAll(dir)
	}()

	meta, err := OpenRaftMeta(dir)
	if err != nil {
		fmt.Println(err)
		return
	}
	assert.Equal(t, meta.GetRaftInit(), uint16(0))
	meta.SetRaftInit(1)
	assert.Equal(t, meta.GetRaftInit(), uint16(1))
}

func TestRafSlotInit(t *testing.T) {
	dir := "/tmp/raftmeta"
	os.MkdirAll(dir, 0755)
	defer func() {
		os.RemoveAll(dir)
	}()

	meta, err := OpenRaftMeta(dir)
	if err != nil {
		fmt.Println(err)
		return
	}
	assert.Equal(t, meta.GetSlotInit(), uint16(0))
	meta.SetSlotInit(1)
	assert.Equal(t, meta.GetSlotInit(), uint16(1))
}

func TestRaftGetAllSlots(t *testing.T) {
	dir := "/tmp/raftmeta"
	os.MkdirAll(dir, 0755)
	defer func() {
		os.RemoveAll(dir)
	}()

	meta, err := OpenRaftMeta(dir)
	if err != nil {
		fmt.Println(err)
		return
	}
	slots := meta.GetAllSlots()
	for i := 0; i < 1024; i++ {
		assert.Equal(t, uint64(0), slots[i])
	}
	initCluster := uint16(200)
	meta.InitAllSlots(initCluster)
	slots = meta.GetAllSlots()
	for i := 0; i < 1024; i++ {
		assert.Equal(t, uint64(initCluster), slots[i])
	}
	for i := 0; i < 1024; i++ {
		meta.SetSlotGroup(i, uint16(i+1))
	}
	slots = meta.GetAllSlots()
	for i := 0; i < 1024; i++ {
		assert.Equal(t, uint64(i+1), slots[i])
	}
}
