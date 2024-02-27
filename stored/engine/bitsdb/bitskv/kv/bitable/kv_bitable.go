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

package kv_bitable

import (
	"fmt"

	"github.com/zuoyebang/bitalostable"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv/kv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/dbconfig"
	"github.com/zuoyebang/bitalostored/stored/internal/bytepools"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

var ErrNotFound = bitalostable.ErrNotFound

type bitableWriteBatch struct {
	wb *bitalostable.Batch
	db *bitalostable.DB
	wo *bitalostable.WriteOptions
}

func (w *bitableWriteBatch) Commit() error {
	return w.wb.Commit(w.wo)
}

func (w *bitableWriteBatch) Put(key []byte, val []byte) error {
	return w.wb.Set(key, val, w.wo)
}

func (w *bitableWriteBatch) PutMultiValue(key []byte, vals ...[]byte) error {
	return w.wb.SetMultiValue(key, vals...)
}

func (w *bitableWriteBatch) Delete(key []byte) error {
	return w.wb.Delete(key, w.wo)
}

func (w *bitableWriteBatch) Clear() {
	w.wb.Reset()
}

func (w *bitableWriteBatch) Close() error {
	err := w.wb.Close()
	w.wb = nil
	return err
}

func (w *bitableWriteBatch) Count() int {
	return int(w.wb.Count())
}

func NewKVStore(sPath string, cfg *dbconfig.Config,
	dataType btools.DataType, dbType int) (kv.IKVStore, error) {
	return openBitableDB(sPath, cfg, dataType, dbType)
}

type KV struct {
	db     *bitalostable.DB
	opts   *bitalostable.Options
	wo     *bitalostable.WriteOptions
	config *dbconfig.Config
}

var _ kv.IKVStore = (*KV)(nil)

func openBitableDB(sPath string, cfg *dbconfig.Config, dataType btools.DataType, dbType int) (kv.IKVStore, error) {
	writeBufferSize := 64 << 20

	l0Size := writeBufferSize
	lopts := make([]bitalostable.LevelOptions, 7)
	for l := 0; l < 7; l++ {
		lopts[l] = bitalostable.LevelOptions{
			Compression:    bitalostable.SnappyCompression,
			BlockSize:      32 * 1024,
			TargetFileSize: int64(l0Size),
		}
		l0Size = l0Size * 2
	}

	opts := &bitalostable.Options{
		MemTableSize:                writeBufferSize,
		MemTableStopWritesThreshold: 16,
		L0CompactionFileThreshold:   32,
		L0CompactionThreshold:       32,
		L0StopWritesThreshold:       128,
		LBaseMaxBytes:               1 << 30,
		MaxOpenFiles:                8000,
		Levels:                      lopts,
		Logger:                      log.GetLogger(),
		Verbose:                     true,
		LogTag:                      fmt.Sprintf("[bitable/%s%s]", dataType.String(), kv.GetDbTypeDir(dbType)),
		DisableWAL:                  cfg.DisableWAL,
		Id:                          kv.GetDbId(dataType, dbType),
		FlushReporter:               cfg.FlushReporterFunc,
	}

	cache := bitalostable.NewCache(0)
	defer cache.Unref()
	opts.Cache = cache

	kvDb := &KV{
		wo:     bitalostable.NoSync,
		opts:   opts,
		config: cfg,
	}

	db, err := bitalostable.Open(sPath, opts)
	if err != nil {
		return nil, err
	}

	kvDb.db = db
	return kvDb, nil
}

func (r *KV) IsNotFound(err error) bool {
	return err == bitalostable.ErrNotFound
}

func (r *KV) Close() error {
	if err := r.db.Close(); err != nil {
		return err
	}
	return nil
}

func (r *KV) Get(key []byte) ([]byte, error) {
	v, closer, err := r.db.Get(key)
	if err != nil {
		if r.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	defer func() {
		if closer != nil {
			_ = closer.Close()
		}
	}()

	return append([]byte{}, v...), nil
}

func (r *KV) GetValue(key []byte) ([]byte, error) {
	v, closer, err := r.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closer != nil {
			_ = closer.Close()
		}
	}()

	return append([]byte{}, v...), nil
}

func (r *KV) GetPools(key []byte) ([]byte, func(), error) {
	v, closer, err := r.db.Get(key)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if closer != nil {
			_ = closer.Close()
		}
	}()

	value, valCloser := bytepools.BytePools.MakeValue(v)
	return value, valCloser, nil
}

func (r *KV) IsExist(key []byte) (bool, error) {
	_, closer, err := r.db.Get(key)
	if err != nil {
		if err == ErrNotFound {
			err = nil
		}
		return false, err
	}
	defer func() {
		if closer != nil {
			_ = closer.Close()
		}
	}()

	return true, nil
}

func (r *KV) MGet(keys ...[]byte) ([][]byte, error) {
	res := make([][]byte, len(keys))

	for i, key := range keys {
		res[i] = nil
		if key != nil {
			v, err := r.Get(key)
			if err == nil && v != nil {
				res[i] = v
			}
		}
	}

	return res, nil
}

func (r *KV) Set(key []byte, value []byte) error {
	return r.db.Set(key, value, r.wo)
}

func (r *KV) Delete(key []byte) error {
	return r.db.Delete(key, r.wo)
}

func (r *KV) Flush() error {
	return r.db.Flush()
}

func (r *KV) AsyncFlush() (<-chan struct{}, error) {
	return r.db.AsyncFlush()
}

func (r *KV) Compact(_ int) {
}

func (r *KV) GetWriteBatch() kv.IWriteBatch {
	return &bitableWriteBatch{
		wb: r.db.NewBatch(),
		db: r.db,
		wo: r.wo,
	}
}

func (r *KV) ForestInfo() kv.ForestInfo {
	return kv.ForestInfo{}
}

func (r *KV) DebugInfo() string {
	return ""
}

func (r *KV) CacheInfo() string {
	return ""
}

func (r *KV) Id() int {
	return r.db.Id()
}

func (r *KV) SetCheckpointLock(v bool) {
}

func (r *KV) SetCheckpointHighPriority(v bool) {
}

func (r *KV) Checkpoint(destDir string) error {
	opt := bitalostable.WithFlushedWAL()
	return r.db.Checkpoint(destDir, opt)
}

func (r *KV) NewIter(o *kv.IteratorOptions) kv.IIterator {
	ro := &bitalostable.IterOptions{
		LowerBound: o.LowerBound,
		UpperBound: o.UpperBound,
	}
	iter := &bitableIterator{
		it: r.db.NewIter(ro),
	}
	return iter
}

type bitableIterator struct {
	it *bitalostable.Iterator
}

func (i *bitableIterator) Key() []byte {
	return i.it.Key()
}

func (i *bitableIterator) Value() []byte {
	return i.it.Value()
}

func (i *bitableIterator) Valid() bool {
	return i.it.Valid()
}

func (i *bitableIterator) Prev() bool {
	return i.it.Prev()
}

func (i *bitableIterator) Next() bool {
	return i.it.Next()
}

func (i *bitableIterator) First() bool {
	return i.it.First()
}

func (i *bitableIterator) Last() bool {
	return i.it.Last()
}

func (i *bitableIterator) Close() error {
	return i.it.Close()
}

func (i *bitableIterator) SeekGE(key []byte) bool {
	return i.it.SeekGE(key)
}

func (i *bitableIterator) SeekLT(key []byte) bool {
	return i.it.SeekLT(key)
}
