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

package dbconfig

import (
	"sync/atomic"
)

type Config struct {
	DBPath                     string
	DelExpireDataPoolNum       int
	GetNextKeyId               func() uint64
	GetCurrentKeyId            func() uint64
	WriteBufferSize            int
	MaxWriteBufferNum          int
	DisableWAL                 bool
	CacheSize                  int
	CacheHashSize              int
	CompactStartTime           int
	CompactEndTime             int
	BithashGcThreshold         float64
	CompactInterval            int
	BithashCompressionType     int
	EnablePageBlockCompression bool
	PageBlockCacheSize         int
	EnableRaftlogRestore       bool
	KvCheckExpireFunc          func(int, []byte, []byte) bool
	KvTimestampFunc            func([]byte, uint8) (bool, uint64)
	FlushReporterFunc          func(int)
	IOWriteLoadThresholdFunc   func() bool
}

func NewConfigDefault() *Config {
	cfg := &Config{}
	cfg.DelExpireDataPoolNum = 8
	cfg.WriteBufferSize = getDefault(256<<20, cfg.WriteBufferSize)
	cfg.MaxWriteBufferNum = getDefault(8, cfg.MaxWriteBufferNum)
	cfg.CacheSize = getDefault(0, cfg.CacheSize)
	if cfg.GetNextKeyId == nil {
		cfg.GetNextKeyId = DefaultGetNextKeyId
		cfg.GetCurrentKeyId = DefaultGetCurrrentKeyId
	}

	return cfg
}

var DefaultKeyId atomic.Uint64

func DefaultGetNextKeyId() uint64 {
	return DefaultKeyId.Add(1)
}

func DefaultGetCurrrentKeyId() uint64 {
	return DefaultKeyId.Load()
}

func getDefault(d int, s int) int {
	if s <= 0 {
		return d
	}

	return s
}
