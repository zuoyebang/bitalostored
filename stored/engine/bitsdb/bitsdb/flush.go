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

package bitsdb

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv/kv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/dbmeta"
	"github.com/zuoyebang/bitalostored/stored/internal/config"

	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

const FlushLength = 100

var errIndexTooSmall = errors.New("flush index less than last")
var errFlushTypeDisable = errors.New("flush type disable")

type flushData struct {
	reason btools.FlushType
	index  uint64
	dbId   int
}

func (f flushData) String() string {
	return fmt.Sprintf("reason:%s index:%d dbid:%d dbname:%s", f.reason, f.index, f.dbId, kv.GetDbName(f.dbId))
}

type FlushTask struct {
	queue           chan *flushData
	dbs             []kv.IKVStore
	meta            *dbmeta.Meta
	openRaftRestore bool
	lastFlushIndex  atomic.Uint64
	blockSync       atomic.Bool
	flushLock       sync.Mutex
	wg              sync.WaitGroup
	closed          atomic.Bool
}

func newFlushTask(meta *dbmeta.Meta, openRaftRestore bool) *FlushTask {
	task := &FlushTask{
		queue:           make(chan *flushData, FlushLength),
		meta:            meta,
		openRaftRestore: openRaftRestore,
	}

	if openRaftRestore {
		task.lastFlushIndex.Store(meta.GetFlushIndex())
	}

	return task
}

func (task *FlushTask) initTask(bdb *BitsDB) {
	task.dbs = bdb.GetAllDB()

	if task.openRaftRestore {
		task.consume()
	}
}

func (task *FlushTask) raftReset() {
	task.lastFlushIndex.Store(0)
}

func (task *FlushTask) consume() {
	log.Infof("flush task consume is running lastFlushIndex:%d", task.lastFlushIndex.Load())

	task.wg.Add(1)
	go func() {
		defer func() {
			task.wg.Done()

			if r := recover(); r != nil {
				log.Errorf("flush task consume panic [err=%s] [stack=%s]", r, string(debug.Stack()))
				time.Sleep(100 * time.Millisecond)
				task.consume()
				return
			}

			log.Infof("flush task consume quit")
		}()

		for {
			qdata, ok := <-task.queue
			if !ok || task.isClosed() || qdata == nil {
				return
			}

			log.Infof("flush task consume recv data:%s", qdata.String())
			task.SyncFlush(*qdata)
		}
	}()
}

func (task *FlushTask) allowed(data flushData) error {
	if !task.openRaftRestore && (data.reason == btools.FlushTypeDbFlush || data.reason == btools.FlushTypeRemoveLog) {
		return errFlushTypeDisable
	}
	if task.openRaftRestore && config.GlobalConfig.Plugin.OpenRaft && (data.index <= task.lastFlushIndex.Load()) {
		return errIndexTooSmall
	}
	return nil
}

func (task *FlushTask) SyncFlush(data flushData) {
	defer log.Cost("sync flush ", data.String())()

	task.flushLock.Lock()
	defer task.flushLock.Unlock()
	if err := task.allowed(data); err != nil {
		log.Errorf("sync flush not allowed err:%s", err)
		return
	}

	var err error
	for _, db := range task.dbs {
		if task.isBlockedSyncTask() {
			return
		}
		name := kv.GetDbName(db.Id())
		if db.Id() == data.dbId {
			continue
		}
		if err = db.Flush(); err != nil {
			log.Infof("sync flush fail db:%s err:%s", name, err)
			return
		}
		log.Infof("sync flush success db:%s", name)
	}

	task.flushEnd(data.index)
}

func (task *FlushTask) AyncFlush(data flushData) (<-chan struct{}, error) {
	if task.isClosed() {
		return nil, errors.New("task closed")
	}

	log.Infof("async flush start data:%s", data.String())
	task.blockSyncTask()
	task.flushLock.Lock()

	releaseFunc := func() {
		task.flushLock.Unlock()
		task.unblockSyncTask()
	}

	if err := task.allowed(data); err != nil {
		releaseFunc()
		return nil, err
	}

	start := time.Now()
	flushAll := make(chan struct{})
	var waitChans []<-chan struct{}

	for _, db := range task.dbs {
		dbName := kv.GetDbName(db.Id())
		if ch, err := db.AsyncFlush(); err != nil {
			log.Errorf("async flush db:%s err:%s", dbName, err)
		} else {
			if ch != nil {
				log.Infof("async flush wait db:%s", dbName)
				waitChans = append(waitChans, ch)
			} else {
				log.Infof("async flush skip(empty memtable) db:%s", kv.GetDbName(db.Id()))
			}
		}
	}

	go func() {
		defer releaseFunc()

		for _, ch := range waitChans {
			<-ch
		}

		task.flushEnd(data.index)
		close(flushAll)
		log.Infof("async flush end cost:%.3fs data:%s", time.Since(start).Seconds(), data.String())
	}()

	return flushAll, nil
}

func (task *FlushTask) FlushFunc(r btools.FlushType) func(int) {
	return func(id int) {
		task.queue <- &flushData{
			index:  task.meta.GetUpdateIndex(),
			dbId:   id,
			reason: r,
		}
	}
}

func (task *FlushTask) Close() {
	task.closed.Store(true)
	if task.openRaftRestore {
		task.queue <- nil
		task.wg.Wait()
	}
}

func (task *FlushTask) isClosed() bool {
	return task.closed.Load()
}

func (task *FlushTask) flushEnd(flushIndex uint64) {
	if config.GlobalConfig.Plugin.OpenRaft && task.openRaftRestore {
		task.meta.SetFlushIndex(flushIndex)
		task.lastFlushIndex.Store(flushIndex)
	}
}

func (task *FlushTask) blockSyncTask() {
	task.blockSync.Store(true)
}

func (task *FlushTask) unblockSyncTask() {
	task.blockSync.Store(false)
}

func (task *FlushTask) isBlockedSyncTask() bool {
	return task.blockSync.Load()
}
