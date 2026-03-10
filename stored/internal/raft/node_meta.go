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
	"io"
	"os"
	"path"
	"sync"

	"github.com/zuoyebang/bitalostored/stored/internal/config"

	"github.com/zuoyebang/bitalostored/butils/mmap"
)

// file: format
//
//	0-10  applyIndex
const (
	FileSize          = 1024
	FieldUpdateOffset = 0
)

type NodeMeta struct {
	file      *mmap.MMap
	name      string
	mu        sync.RWMutex
	clusterId uint64
}

func OpenNodeMeta(dir string, clusterId uint64) (*NodeMeta, error) {
	filePath := getMetaFilePath(dir, clusterId)
	file, err := mmap.Open(filePath, FileSize)
	if err != nil {
		return nil, err
	}
	m := &NodeMeta{
		file:      file,
		name:      filePath,
		clusterId: clusterId,
	}
	return m, nil
}

func getMetaFilePath(dir string, clusterId uint64) string {
	return path.Join(dir, config.GetRaftNodeMetaFile(clusterId))
}

func (m *NodeMeta) Checkpoint(dstDir string) error {
	srcPath := m.name
	dstPath := getMetaFilePath(dstDir, m.clusterId)

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

func (m *NodeMeta) GetUpdateIndex() uint64 {
	return m.file.ReadUInt64At(FieldUpdateOffset)
}

func (m *NodeMeta) SetUpdateIndex(u uint64) {
	m.file.WriteUInt64At(u, FieldUpdateOffset)
}

func (m *NodeMeta) Close() {
	m.file.Close()
}

func (m *NodeMeta) Remove() error {
	return os.Remove(m.name)
}
