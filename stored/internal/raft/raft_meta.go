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
	"encoding/binary"
	"os"
	"path"
	"sync"

	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

// file: format
//
//	0-1 FlagRaftInit
//	2-3 FlagSlotInit

const (
	FieldRaftInitV7Offset = 0
	FieldSlotInitOffset   = 2
	SlotGroupOffset       = 1024 // groupIds (slot=0~1023)
)

type RaftMeta struct {
	file      *os.File
	name      string
	mu        sync.RWMutex
	clusterId uint64
}

func OpenRaftMeta(dir string) (*RaftMeta, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Errorf("open raft meta:%s err:%s", dir, err)
		return nil, err
	}
	filePath := getRaftMetaPath(dir)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}
	m := &RaftMeta{
		file: file,
		name: filePath,
	}
	return m, nil
}

func getRaftMetaPath(dir string) string {
	return path.Join(dir, config.GetRaftMetaFile())
}

func (m *RaftMeta) GetSlotInit() uint16 {
	var b [2]byte
	_, err := m.file.ReadAt(b[:], FieldSlotInitOffset)
	if err != nil {
		return 0
	}
	return binary.LittleEndian.Uint16(b[:])
}

func (m *RaftMeta) SetSlotInit(v uint16) {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], v)
	m.file.WriteAt(b[:], FieldSlotInitOffset)
}

func (m *RaftMeta) InitAllSlots(clusterId uint16) {
	var b [2048]byte
	pos := 0
	for i := 0; i < 1024; i++ {
		binary.LittleEndian.PutUint16(b[pos:pos+2], clusterId)
		pos += 2
	}
	m.file.WriteAt(b[:], SlotGroupOffset)
}

func (m *RaftMeta) SetSlotGroup(slotId int, clusterId uint16) {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], clusterId)
	offset := int64(SlotGroupOffset + slotId*2)
	m.file.WriteAt(b[:], offset)
}

func (m *RaftMeta) GetSlotGroup(slotId int) uint16 {
	var b [2]byte
	offset := SlotGroupOffset + slotId*2
	_, err := m.file.ReadAt(b[:], int64(offset))
	if err != nil {
		return 0
	}
	return binary.LittleEndian.Uint16(b[:])
}

func (m *RaftMeta) GetAllSlots() []uint64 {
	var gid uint16
	var b [2048]byte
	groups := make([]uint64, 0, 1024)
	_, err := m.file.ReadAt(b[:], SlotGroupOffset)
	pos := 0
	for i := 0; i < 1024; i++ {
		if err != nil {
			gid = 0
		} else {
			gid = binary.LittleEndian.Uint16(b[pos : pos+2])
		}
		groups = append(groups, uint64(gid))
		pos += 2
	}
	return groups
}

func (m *RaftMeta) GetRaftInit() uint16 {
	var b [2]byte
	_, err := m.file.ReadAt(b[:], FieldRaftInitV7Offset)
	if err != nil {
		return 0
	}
	return binary.LittleEndian.Uint16(b[:])
}

func (m *RaftMeta) SetRaftInit(n uint16) {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], n)
	m.file.WriteAt(b[:], FieldRaftInitV7Offset)
}

func (m *RaftMeta) Close() {
	m.file.Close()
}
