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

package kv_bitalosdb

import (
	"encoding/binary"
	"fmt"

	"github.com/zuoyebang/bitalosdb"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv/kv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/dbconfig"
	"github.com/zuoyebang/bitalostored/stored/internal/bytepools"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

var ErrNotFound = bitalosdb.ErrNotFound

type bitalosdbWriteBatch struct {
	wb *bitalosdb.BatchBitower
	db *bitalosdb.DB
	wo *bitalosdb.WriteOptions
}

func (w *bitalosdbWriteBatch) Commit() error {
	return w.wb.Commit(w.wo)
}

func (w *bitalosdbWriteBatch) Put(key []byte, val []byte) error {
	return w.wb.Set(key, val, w.wo)
}

func (w *bitalosdbWriteBatch) PutMultiValue(key []byte, vals ...[]byte) error {
	return w.wb.SetMultiValue(key, vals...)
}

func (w *bitalosdbWriteBatch) PutPrefixDeleteKey(key []byte) error {
	return w.wb.PrefixDeleteKeySet(key, w.wo)
}

func (w *bitalosdbWriteBatch) Delete(key []byte) error {
	return w.wb.Delete(key, w.wo)
}

func (w *bitalosdbWriteBatch) Clear() {
	w.wb.Reset()
}

func (w *bitalosdbWriteBatch) Close() error {
	err := w.wb.Close()
	w.wb = nil
	return err
}

func (w *bitalosdbWriteBatch) Count() int {
	return int(w.wb.Count())
}

func NewKVStore(sPath string, cfg *dbconfig.Config, dataType btools.DataType, dbType int) (kv.IKVStore, error) {
	return openBitalosDB(sPath, cfg, dataType, dbType)
}

type KV struct {
	db     *bitalosdb.DB
	opts   *bitalosdb.Options
	wo     *bitalosdb.WriteOptions
	config *dbconfig.Config
}

var _ kv.IKVStore = (*KV)(nil)

func openBitalosDB(dirname string, cfg *dbconfig.Config, dataType btools.DataType, dbType int) (kv.IKVStore, error) {
	compactOpt := bitalosdb.CompactEnv{
		StartHour:     cfg.CompactStartTime,
		EndHour:       cfg.CompactEndTime,
		DeletePercent: cfg.BithashGcThreshold,
		Interval:      cfg.CompactInterval,
	}

	opts := &bitalosdb.Options{
		CompressionType:             cfg.BithashCompressionType,
		MemTableSize:                cfg.WriteBufferSize,
		MemTableStopWritesThreshold: cfg.MaxWriteBufferNum,
		Logger:                      log.GetLogger(),
		Verbose:                     true,
		AutoCompact:                 true,
		CompactInfo:                 compactOpt,
		DataType:                    dataType.String(),
		DisableWAL:                  cfg.DisableWAL,
		UseBithash:                  false,
		UseBitable:                  false,
		UseMapIndex:                 true,
		FlushReporter:               cfg.FlushReporterFunc,
		Id:                          kv.GetDbId(dataType, dbType),
		UsePrefixCompress:           true,
		UseBlockCompress:            cfg.EnablePageBlockCompression,
		BlockCacheSize:              int64(cfg.PageBlockCacheSize),
		IOWriteLoadThresholdFunc:    cfg.IOWriteLoadThresholdFunc,
		BytesPerSync:                1 << 20,
		DeleteFileInternal:          8,
		KvCheckExpireFunc:           nil,
		KvTimestampFunc:             nil,
		KeyPrefixDeleteFunc:         nil,
	}

	if dataType == btools.ZSET && dbType == kv.DB_TYPE_INDEX {
		opts.DataType += kv.GetDbTypeDir(dbType)
		opts.LogTag = fmt.Sprintf("[bitalosdb/%s]", opts.DataType)
	} else {
		opts.LogTag = fmt.Sprintf("[bitalosdb/%s%s]", opts.DataType, kv.GetDbTypeDir(dbType))
	}

	opts.KeyHashFunc = func(k []byte) int {
		return int(binary.LittleEndian.Uint16(k[0:2]))
	}

	if dbType == kv.DB_TYPE_META {
		opts.UseBithash = true
		opts.KvTimestampFunc = cfg.KvTimestampFunc
		opts.KvCheckExpireFunc = cfg.KvCheckExpireFunc
	} else {
		if dataType == btools.HASH || dataType == btools.LIST {
			opts.UseBithash = true
		}
		if (dataType == btools.ZSET && dbType == kv.DB_TYPE_INDEX) || dataType == btools.LIST {
			opts.UseMapIndex = false
		}
		if dataType == btools.ZSET && dbType == kv.DB_TYPE_DATA {
			opts.UsePrefixCompress = false
		} else {
			opts.KeyPrefixDeleteFunc = func(k []byte) uint64 {
				if len(k) < 10 {
					return 0
				}
				return binary.LittleEndian.Uint64(k[2:10])
			}
		}
	}

	kvDb := &KV{
		wo:     bitalosdb.NoSync,
		opts:   opts,
		config: cfg,
	}

	db, err := bitalosdb.Open(dirname, opts)
	if err != nil {
		return nil, err
	}

	log.Infof("open bitalosdb success dirname:%s useBithash:%v useMapIndex:%v", dirname, opts.UseBithash, opts.UseMapIndex)

	kvDb.db = db
	return kvDb, nil
}

func (r *KV) IsNotFound(err error) bool {
	return err == bitalosdb.ErrNotFound
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
			closer()
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
			closer()
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
			closer()
		}
	}()

	value, valCloser := bytepools.BytePools.MakeValue(v)
	return value, valCloser, nil
}

func (r *KV) IsExist(key []byte) (bool, error) {
	exist, err := r.db.Exist(key)
	if err == ErrNotFound {
		err = nil
	}

	return exist, err
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

func (r *KV) Compact(jobId int) {
	r.db.CompactBitree(jobId)
}

func (r *KV) GetWriteBatch() kv.IWriteBatch {
	return &bitalosdbWriteBatch{
		wb: r.db.NewBatchBitower(),
		db: r.db,
		wo: r.wo,
	}
}

func (r *KV) MetricsInfo() kv.MetricsInfo {
	return kv.MetricsInfo(r.db.MetricsInfo())
}

func (r *KV) DebugInfo() string {
	return r.db.DebugInfo()
}

func (r *KV) CacheInfo() string {
	return r.db.CacheInfo()
}

func (r *KV) Id() int {
	return r.db.Id()
}

func (r *KV) SetAutoCompact(val bool) {
	r.db.SetAutoCompact(val)
}

func (r *KV) SetCheckpointLock(v bool) {
	r.db.SetCheckpointLock(v)
}

func (r *KV) SetCheckpointHighPriority(v bool) {
	r.db.SetCheckpointHighPriority(v)
}

func (r *KV) Checkpoint(destDir string) error {
	return r.db.Checkpoint(destDir)
}

func (r *KV) NewIter(o *kv.IteratorOptions) kv.IIterator {
	ro := &bitalosdb.IterOptions{
		LowerBound:   o.LowerBound,
		UpperBound:   o.UpperBound,
		SlotId:       o.SlotId,
		IsAll:        o.IsAll,
		DisableCache: o.DisableCache,
	}
	iter := &bitalosdbIterator{
		it: r.db.NewIter(ro),
	}
	return iter
}

type bitalosdbIterator struct {
	it *bitalosdb.Iterator
}

func (i *bitalosdbIterator) Key() []byte {
	return i.it.Key()
}

func (i *bitalosdbIterator) Value() []byte {
	return i.it.Value()
}

func (i *bitalosdbIterator) Valid() bool {
	return i.it.Valid()
}

func (i *bitalosdbIterator) Prev() bool {
	return i.it.Prev()
}

func (i *bitalosdbIterator) Next() bool {
	return i.it.Next()
}

func (i *bitalosdbIterator) First() bool {
	return i.it.First()
}

func (i *bitalosdbIterator) Last() bool {
	return i.it.Last()
}

func (i *bitalosdbIterator) Close() error {
	return i.it.Close()
}

func (i *bitalosdbIterator) SeekGE(key []byte) bool {
	return i.it.SeekGE(key)
}

func (i *bitalosdbIterator) SeekLT(key []byte) bool {
	return i.it.SeekLT(key)
}
