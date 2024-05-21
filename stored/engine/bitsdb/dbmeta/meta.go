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

package dbmeta

import (
	"io"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalostored/butils/mmap"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

// file: format
//  0-8  applyindex
//  8-16 snapshotorder
// 16-24 snapshotindex
// 24-32 snapshotstamp
// 32-40 snapshotindex
// 40-48 snapshotstamp
// 48-56 snapshotindex
// 56-64 snapshotstamp

// 128-136 migratestatus
// 136-148 migrateslotid

// 256-258 compress_type
// 258-260 database_type
// 260-268 keyId
// 268-276 flushIndex

const (
	FileSize                 = 1024
	FieldUIntLenth           = 8
	FieldKeyUniqIdGap        = 10000
	RestartFieldKeyUniqIdGap = 1000000

	FieldUpdateOffset = 0

	FieldSnapshotOffset = 16
	FieldSnapshotLength = 16
	FieldSnapshotCount  = 1

	FieldMigrateOffset = 128

	FieldCompressTypeOffset = 256
	FieldDatabaseTypeOffset = 258
	FieldKeyUniqIdOffset    = 260
	FieldFlushIndexOffset   = 268
)

const MetaFileName = "BSMANIFEST"

type Meta struct {
	file  *mmap.MMap
	KeyId atomic.Uint64
	name  string
	mu    sync.RWMutex
}

func OpenMeta(dir string) (*Meta, error) {
	filePath := getMetaFilePath(dir)
	file, err := mmap.Open(filePath, FileSize)
	if err != nil {
		return nil, err
	}
	m := &Meta{
		file: file,
		name: filePath,
	}
	m.InitKeyUniqId()
	return m, nil
}

func getMetaFilePath(dir string) string {
	return path.Join(dir, MetaFileName)
}

func (m *Meta) Checkpoint(dstDir string) error {
	srcPath := m.name
	dstPath := getMetaFilePath(dstDir)

	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dst.Close()

	if _, err = io.Copy(dst, src); err != nil {
		return err
	}
	return dst.Sync()
}

func (m *Meta) GetUpdateIndex() uint64 {
	return m.file.ReadUInt64At(FieldUpdateOffset)
}

func (m *Meta) SetUpdateIndex(u uint64) {
	m.file.WriteUInt64At(u, FieldUpdateOffset)
}

func (m *Meta) GetFlushIndex() uint64 {
	return m.file.ReadUInt64At(FieldFlushIndexOffset)
}

func (m *Meta) SetFlushIndex(idx uint64) {
	m.file.WriteUInt64At(idx, FieldFlushIndexOffset)
}

func (m *Meta) GetSnapshotOrder() uint64 {
	return m.file.ReadUInt64At(FieldSnapshotOffset - FieldUIntLenth)
}

func (m *Meta) GetSnapshotIndex() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	order := m.file.ReadUInt64At(FieldSnapshotOffset - FieldUIntLenth)
	offset := FieldSnapshotOffset + FieldSnapshotLength*(order%FieldSnapshotCount)
	return m.file.ReadUInt64At(int(offset))
}

func (m *Meta) GetSnapshotStamp() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	order := m.file.ReadUInt64At(FieldSnapshotOffset - FieldUIntLenth)
	offset := FieldSnapshotOffset + FieldSnapshotLength*(order%FieldSnapshotCount) + FieldUIntLenth
	return m.file.ReadInt64At(int(offset))
}

func (m *Meta) SetSnapshotIndex(u uint64) uint64 {
	if u == m.GetSnapshotIndex() {
		return 0
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	order := m.file.ReadUInt64At(FieldSnapshotOffset-FieldUIntLenth) + 1
	offset := FieldSnapshotOffset + FieldSnapshotLength*(order%FieldSnapshotCount)
	now := time.Now().Unix()
	last := m.file.ReadUInt64At(int(offset))
	log.Info("setsnapshot index: ", u, " order: ", order, " offset: ", offset, ": stamp: ", now, " last: ", last)

	m.file.WriteUInt64At(order, FieldSnapshotOffset-FieldUIntLenth)
	m.file.WriteUInt64At(u, int(offset))
	m.file.WriteInt64At(now, int(offset+FieldUIntLenth))
	return last
}

func (m *Meta) GetMigrateStatus() uint64 {
	return m.file.ReadUInt64At(FieldMigrateOffset)
}

func (m *Meta) SetMigrateStatus(u uint64) {
	m.file.WriteUInt64At(u, FieldMigrateOffset)
}

func (m *Meta) GetMigrateSlotid() uint64 {
	return m.file.ReadUInt64At(FieldMigrateOffset + FieldUIntLenth)
}

func (m *Meta) SetMigrateSlotid(u uint64) {
	m.file.WriteUInt64At(u, FieldMigrateOffset+FieldUIntLenth)
}

func (m *Meta) ClearSnapshot() {
	for i := FieldSnapshotOffset - FieldUIntLenth; i < FieldMigrateOffset; i += FieldUIntLenth {
		m.file.WriteUInt64At(0, i)
	}
}

func (m *Meta) GetBitalosdbCompressTypeCfg() (isSet bool, t uint16) {
	u := m.file.ReadUInt16At(FieldCompressTypeOffset)
	if u > 0 {
		isSet = true
		t = u - 1
	} else {
		isSet = false
		t = 0
	}
	return
}

func (m *Meta) SetBitalosdbCompressTypeCfg(t uint16) {
	m.file.WriteUInt16At(t+1, FieldCompressTypeOffset)
}

func (m *Meta) GetBitalosdbDatabaseTypeCfg() (isSet bool, t uint16) {
	u := m.file.ReadUInt16At(FieldDatabaseTypeOffset)
	if u > 0 {
		isSet = true
		t = u - 1
	} else {
		isSet = false
		t = 0
	}
	return
}

func (m *Meta) SetBitalosdbDatabaseTypeCfg(t uint16) {
	m.file.WriteUInt16At(t+1, FieldDatabaseTypeOffset)
}

func (m *Meta) InitKeyUniqId() {
	m.mu.Lock()
	defer m.mu.Unlock()

	keyId := m.file.ReadUInt64At(FieldKeyUniqIdOffset) + RestartFieldKeyUniqIdGap
	m.KeyId.Store(keyId)
	m.file.WriteUInt64At(keyId, FieldKeyUniqIdOffset)
}

func (m *Meta) GetNextKeyUniqId() uint64 {
	newKeyId := m.KeyId.Add(1)
	if newKeyId%FieldKeyUniqIdGap == 0 {
		m.mu.Lock()
		m.file.WriteUInt64At(newKeyId+FieldKeyUniqIdGap, FieldKeyUniqIdOffset)
		m.mu.Unlock()
	}
	return newKeyId
}

func (m *Meta) GetCurrentKeyUniqId() uint64 {
	return m.KeyId.Load()
}

func (m *Meta) GetDiskKeyUniqId() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.file.ReadUInt64At(FieldKeyUniqIdOffset)
}

func (m *Meta) RaftReset() {
	m.SetUpdateIndex(0)
	m.SetFlushIndex(0)
	m.SetSnapshotIndex(0)
	m.SetMigrateSlotid(0)
	m.SetMigrateStatus(0)
	m.ClearSnapshot()
}

func (m *Meta) Close() {
	m.file.Close()
}
