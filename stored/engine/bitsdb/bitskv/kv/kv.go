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

package kv

import (
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/dbconfig"
)

type Factory func(string, *dbconfig.Config, btools.DataType, int) (IKVStore, error)

type IWriteBatch interface {
	Commit() error
	Put(key []byte, val []byte) error
	PutMultiValue(key []byte, vals ...[]byte) error
	PutPrefixDeleteKey(key []byte) error
	Delete(key []byte) error
	Clear()
	Close() error
	Count() int
}

type IIterator interface {
	Key() []byte
	Value() []byte
	Valid() bool
	Close() error
	Prev() bool
	Next() bool
	First() bool
	Last() bool
	SeekGE(key []byte) bool
	SeekLT(key []byte) bool
}

type IKVStore interface {
	Close() error
	Get(key []byte) ([]byte, error)
	GetValue(key []byte) ([]byte, error)
	GetPools(key []byte) ([]byte, func(), error)
	IsExist(key []byte) (bool, error)
	MGet(keys ...[]byte) ([][]byte, error)
	Set(key []byte, value []byte) error
	Delete(key []byte) error
	IsNotFound(err error) bool
	Flush() error
	AsyncFlush() (<-chan struct{}, error)
	NewIter(opts *IteratorOptions) IIterator
	GetWriteBatch() IWriteBatch
	Compact(jobId int)
	DebugInfo() string
	CacheInfo() string
	MetricsInfo() MetricsInfo
	SetCheckpointLock(lock bool)
	SetCheckpointHighPriority(lock bool)
	Checkpoint(destDir string) error
	Id() int
	SetAutoCompact(bool)
}

type MetricsInfo struct {
	FlushMemTime       int64 `json:"-"`
	BithashFileTotal   int   `json:"bithash_file_total"`
	BithashKeyTotal    int   `json:"bithash_key_total"`
	BithashDelKeyTotal int   `json:"bithash_del_key_total"`
}

type IteratorOptions struct {
	LowerBound   []byte
	UpperBound   []byte
	KeyHash      uint32
	SlotId       uint32
	IsAll        bool
	DisableCache bool
}
