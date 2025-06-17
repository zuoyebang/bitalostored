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

package bitsdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/zuoyebang/bitalostored/butils"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/hash"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/list"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/rstring"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/set"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/zset"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv/kv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/dbconfig"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/dbmeta"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"
)

const (
	delExpireTimeoutSecond = 2
	delExpireKeepTimeout   = 5
)

type BitsDB struct {
	HashObj   *hash.HashObject
	StringObj *rstring.StringObject
	ListObj   *list.ListObject
	SetObj    *set.SetObject
	ZsetObj   *zset.ZSetObject

	baseDb                *base.BaseDB
	isDelExpireRun        atomic.Int32
	isCheckpoint          atomic.Bool
	ckpExpLock            sync.Mutex
	flushTask             *FlushTask
	isRaftRestore         bool
	statQPS               atomic.Uint64
	delExpireKeys         atomic.Uint64
	delExpireZsetKeys     atomic.Uint64
	delExpireTimeoutCount uint8
}

func NewBitsDB(cfg *dbconfig.Config, meta *dbmeta.Meta) (*BitsDB, error) {
	btools.SetDefineVarFromCfg()
	flushTask := newFlushTask(meta, cfg.EnableRaftlogRestore)
	if cfg.EnableRaftlogRestore {
		cfg.FlushReporterFunc = flushTask.FlushFunc(btools.FlushTypeDbFlush)
	} else {
		cfg.FlushReporterFunc = nil
	}

	bdb := &BitsDB{
		flushTask:     flushTask,
		isRaftRestore: cfg.EnableRaftlogRestore,
	}
	cfg.IOWriteLoadThresholdFunc = bdb.CheckIOWriteLoadThreshold
	cfg.KvCheckExpireFunc = bdb.CheckKvExpire
	cfg.KvTimestampFunc = bdb.GetMetaValueTimestamp
	baseDb, err := base.NewBaseDB(cfg)
	if err != nil {
		return nil, err
	}

	bdb.baseDb = baseDb
	bdb.StringObj = rstring.NewStringObject(baseDb, cfg)
	bdb.ZsetObj = zset.NewZSetObject(baseDb, cfg)
	bdb.HashObj = hash.NewHashObject(baseDb, cfg)
	bdb.SetObj = set.NewSetObject(baseDb, cfg)
	bdb.ListObj = list.NewListObject(baseDb, cfg)
	bdb.flushTask.initTask(bdb)
	bdb.baseDb.SetReady()
	return bdb, nil
}

func (bdb *BitsDB) IsReady() bool {
	return bdb.baseDb.IsReady()
}

func (bdb *BitsDB) IsOpenRaftRestore() bool {
	return bdb.isRaftRestore
}

func (bdb *BitsDB) IsDelExpireRun() int {
	return int(bdb.isDelExpireRun.Load())
}

func (bdb *BitsDB) IsCheckpointHighPriority() bool {
	return bdb.isCheckpoint.Load()
}

func (bdb *BitsDB) SetCheckpointHighPriority(v bool) {
	bdb.isCheckpoint.Store(v)
}

func (bdb *BitsDB) CheckpointExpireLock(lock bool) {
	if lock {
		bdb.ckpExpLock.Lock()
	} else {
		bdb.ckpExpLock.Unlock()
	}
}

func (bdb *BitsDB) FlushAllDB() {
	var waitChs []<-chan struct{}
	dbs := bdb.GetAllDB()

	for i := range dbs {
		if ch, err := dbs[i].AsyncFlush(); err == nil && ch != nil {
			waitChs = append(waitChs, ch)
		}
	}

	for i := range waitChs {
		<-waitChs[i]
	}
}

func (bdb *BitsDB) ClearCache() {
	bdb.baseDb.ClearCache()
}

func (bdb *BitsDB) Close() {
	log.Infof("bitsDB Close start")
	bdb.baseDb.FlushBitmap()
	bdb.Flush(btools.FlushTypeDbClose, 0)
	bdb.flushTask.Close()

	bdb.HashObj.Close()
	bdb.ListObj.Close()
	bdb.SetObj.Close()
	bdb.ZsetObj.Close()
	bdb.baseDb.Close()
	log.Infof("bitsDB Close finish")
}

func (bdb *BitsDB) SetAutoCompact(val bool) {
	dbs := bdb.GetAllDB()
	for _, db := range dbs {
		db.SetAutoCompact(val)
	}
}

func (bdb *BitsDB) GetAllDB() []kv.IKVStore {
	dbs := bdb.baseDb.GetAllDB()
	dbs = append(dbs, bdb.HashObj.GetAllDB()...)
	dbs = append(dbs, bdb.ListObj.GetAllDB()...)
	dbs = append(dbs, bdb.SetObj.GetAllDB()...)
	dbs = append(dbs, bdb.ZsetObj.GetAllDB()...)
	return dbs
}

func (bdb *BitsDB) Compact() {
	bdb.baseDb.DB.CompactDB()
	bdb.HashObj.DataDb.CompactDB()
	bdb.ListObj.DataDb.CompactDB()
	bdb.SetObj.DataDb.CompactDB()
	bdb.ZsetObj.DataDb.CompactDB()
}

func (bdb *BitsDB) CompactBitree() {
	bdb.baseDb.DB.CompactBitree()
	bdb.HashObj.DataDb.CompactBitree()
	bdb.ListObj.DataDb.CompactBitree()
	bdb.SetObj.DataDb.CompactBitree()
	bdb.ZsetObj.DataDb.CompactBitree()
}

func (bdb *BitsDB) CompactExpire(start, end []byte) error {
	return bdb.baseDb.CompactExpire(start, end)
}

func (bdb *BitsDB) DebugInfo() []byte {
	var buf bytes.Buffer

	bdb.baseDb.DB.GetMetaDbDebugInfo()
	buf.Write(bdb.baseDb.DB.DebugInfo.Marshal())

	bdb.HashObj.DataDb.GetDataDbDebugInfo()
	buf.Write(bdb.HashObj.DataDb.DebugInfo.Marshal())

	bdb.ListObj.DataDb.GetDataDbDebugInfo()
	buf.Write(bdb.ListObj.DataDb.DebugInfo.Marshal())

	bdb.SetObj.DataDb.GetDataDbDebugInfo()
	buf.Write(bdb.SetObj.DataDb.DebugInfo.Marshal())

	bdb.ZsetObj.DataDb.GetDataDbDebugInfo()
	buf.Write(bdb.ZsetObj.DataDb.DebugInfo.Marshal())

	bdb.ZsetObj.DataDb.GetIndexDbDebugInfo()
	buf.Write(bdb.ZsetObj.DataDb.DebugInfo.Marshal())

	return buf.Bytes()
}

func (bdb *BitsDB) CacheInfo() []byte {
	lruCacheInfo := bdb.baseDb.CacheInfo()

	var buf bytes.Buffer
	buf.WriteString(lruCacheInfo)

	return buf.Bytes()
}

func (bdb *BitsDB) CheckpointPrepareForBitalosdb(v bool) {
	dbs := []*bitskv.DB{
		bdb.baseDb.DB,
		bdb.HashObj.DataDb,
		bdb.ListObj.DataDb,
		bdb.SetObj.DataDb,
		bdb.ZsetObj.DataDb,
	}

	for _, db := range dbs {
		db.SetCheckpointHighPriority(v)
	}
	for _, db := range dbs {
		db.SetCheckpointLock(v)
	}
}

func (bdb *BitsDB) Checkpoint(dir string) error {
	var stringStatus, hashStatus, listStatus, setStatus, zsetStatus bool

	wg := sync.WaitGroup{}
	for _, datatype := range btools.DataTypeList {
		wg.Add(1)
		go func(dt btools.DataType) {
			defer wg.Done()
			var err error
			switch dt {
			case btools.STRING:
				err = bdb.baseDb.DB.Checkpoint(dir)
				if err == nil {
					stringStatus = true
				}
			case btools.HASH:
				err = bdb.HashObj.CheckpointDataDb(dir)
				if err == nil {
					hashStatus = true
				}
			case btools.LIST:
				err = bdb.ListObj.CheckpointDataDb(dir)
				if err == nil {
					listStatus = true
				}
			case btools.SET:
				err = bdb.SetObj.CheckpointDataDb(dir)
				if err == nil {
					setStatus = true
				}
			case btools.ZSET:
				err = bdb.ZsetObj.CheckpointDataDb(dir)
				if err == nil {
					zsetStatus = true
				}
			}

			if err != nil {
				log.Errorf("checkpoint fail dt:%s err:%s", dt, err.Error())
			}
		}(datatype)
	}
	wg.Wait()

	if stringStatus && hashStatus && listStatus && setStatus && zsetStatus {
		return nil
	}

	return errors.New("checkpoint failed err")
}

func (bdb *BitsDB) BitskvUsage(bu *BitsUsage) {
	if bdb == nil || !bdb.IsReady() {
		return
	}

	bu.metaUsage.SetDataDiskSize(butils.GetDirSize(config.GetBitalosMetaDbPath()))
	bu.metaUsage.SetDataStats(bdb.StringObj.MetaStats())
	bu.metaUsage.SetExpireDiskSize(butils.GetDirSize(config.GetBitalosExireDbPath()))

	bu.listUsage.SetDataDiskSize(butils.GetDirSize(config.GetBitalosDataDbPath(btools.ListName)))
	bu.hashUsage.SetDataDiskSize(butils.GetDirSize(config.GetBitalosDataDbPath(btools.HashName)))
	bu.setUsage.SetDataDiskSize(butils.GetDirSize(config.GetBitalosDataDbPath(btools.SetName)))
	bu.zsetUsage.SetDataDiskSize(butils.GetDirSize(config.GetBitalosDataDbPath(btools.ZSetName)))
	bu.zsetUsage.SetIndexDiskSize(butils.GetDirSize(config.GetBitalosIndexDbPath()))

	bu.listUsage.SetDataStats(bdb.ListObj.DataStats())
	bu.hashUsage.SetDataStats(bdb.HashObj.DataStats())
	bu.setUsage.SetDataStats(bdb.SetObj.DataStats())
	bu.zsetUsage.SetDataStats(bdb.ZsetObj.DataStats())
	bu.zsetUsage.SetIndexStats(bdb.ZsetObj.IndexStats())

	bu.UpdateCache()
}

func (bdb *BitsDB) Flush(reason btools.FlushType, compactIndex uint64) {
	task := flushData{
		index:  0,
		reason: reason,
		dbId:   kv.DB_ID_NONE,
	}

	if config.GlobalConfig.Plugin.OpenRaft {
		if compactIndex > 0 {
			if flushIndex := bdb.flushTask.meta.GetFlushIndex(); compactIndex <= flushIndex {
				log.Infof("donot async flush compactIndex:%d flushIndex:%d", compactIndex, flushIndex)
				return
			}
		}
		task.index = bdb.flushTask.meta.GetUpdateIndex()
	}

	if flushCh, err := bdb.flushTask.AsyncFlush(task); err != nil {
		log.Errorf("async flush err:%s", err)
	} else {
		<-flushCh
	}
}

func (bdb *BitsDB) RaftReset() {
	bdb.flushTask.raftReset()
}

func (bdb *BitsDB) GetMetaValueTimestamp(val []byte, t uint8) (bool, uint64) {
	if t == 2 {
		return false, uint64(tclock.GetTimestampMilli())
	}

	if len(val) < base.MetaStringValueLen {
		return false, 0
	}

	dt := btools.DataType(val[0])
	if dt != btools.STRING {
		return false, 0
	}

	timestamp := binary.BigEndian.Uint64(val[1:])
	return true, timestamp
}

func (bdb *BitsDB) SetQPS(qps uint64) {
	bdb.statQPS.Store(qps)
}

func (bdb *BitsDB) CheckIOWriteLoadThreshold() bool {
	qps := bdb.statQPS.Load()
	return qps < btools.MaxIOWriteLoadQPS
}
