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
	"encoding/binary"
	"runtime/debug"
	"time"

	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv/kv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"
)

func (bdb *BitsDB) CheckKvExpire(dbId int, key, value []byte) bool {
	switch dbId {
	case kv.DB_ID_META:
		exist, timestamp := bdb.GetMetaValueTimestamp(value, 1)
		if !exist || timestamp == 0 {
			return false
		}
		return timestamp <= uint64(tclock.GetTimestampMilli())
	default:
		return false
	}
}

func (bdb *BitsDB) ScanDeleteExpireDb(jobId uint64) {
	if !bdb.IsReady() || bdb.IsCheckpointHighPriority() {
		return
	}

	if !bdb.isDelExpireRun.CompareAndSwap(0, 1) {
		return
	}
	defer func() {
		bdb.isDelExpireRun.Store(0)
		if r := recover(); r != nil {
			log.Errorf("[DELEXPIRE %d] panic err:%s stack:%s", jobId, r, string(debug.Stack()))
			return
		}
	}()

	start := time.Now()
	delKeyNum := 0
	bdb.delExpireKeys.Store(0)
	bdb.delExpireZsetKeys.Store(0)
	log.Infof("[DELEXPIRE %d] scan delete start", jobId)

	bdb.CheckpointExpireLock(true)
	defer bdb.CheckpointExpireLock(false)

	var nowTimeBuf [8]byte
	nowTime := uint64(tclock.GetTimestampMilli())
	binary.BigEndian.PutUint64(nowTimeBuf[:], nowTime+1)
	iterOpts := &bitskv.IterOptions{
		IsAll:      true,
		UpperBound: nowTimeBuf[:],
	}
	it := bdb.baseDb.DB.NewIteratorExpire(iterOpts)
	defer it.Close()

	for it.First(); it.Valid(); it.Next() {
		if !bdb.IsReady() || bdb.IsCheckpointHighPriority() {
			break
		}

		iterKey := it.RawKey()
		timestamp, dataType, keyVersion, keyKind, key, err := base.DecodeExpireKey(iterKey)
		if err != nil {
			log.Errorf("[DELEXPIRE %d] decode expireKey fail key:%s err:%s", jobId, string(iterKey), err)
			continue
		}

		if timestamp > nowTime || delKeyNum >= base.DeleteMixKeyMaxNum {
			break
		}

		keyHash := hash.Fnv32(key)
		switch dataType {
		case btools.HASH:
			err = bdb.HashObj.DeleteDataKeyByExpire(keyVersion, keyHash)
		case btools.SET:
			err = bdb.SetObj.DeleteDataKeyByExpire(keyVersion, keyHash)
		case btools.LIST:
			err = bdb.ListObj.DeleteDataKeyByExpire(keyVersion, keyHash)
		case btools.ZSET:
			err = bdb.ZsetObj.DeleteZsetIndexKeyByExpire(keyVersion, keyHash)
			if err == nil {
				err = bdb.ZsetObj.DeleteDataKeyByExpire(keyVersion, keyHash)
			}
		case btools.ZSETOLD:
			finished, zetDelCnt, err := bdb.ZsetObj.DeleteZsetOldKeyByExpire(keyVersion, keyKind, keyHash)
			if err != nil {
				continue
			}
			bdb.delExpireZsetKeys.Add(zetDelCnt)
			if !finished {
				continue
			}
		default:
			err = errn.ErrDataType
		}
		if err == nil {
			err = bdb.baseDb.DeleteExpireKey(iterKey)
		}
		if err != nil {
			log.Errorf("[DELEXPIRE %d] delete key fail dt:%s err:%s", jobId, dataType, err)
			continue
		}

		bdb.delExpireKeys.Add(1)
		delKeyNum++
	}

	log.Infof("[DELEXPIRE %d] scan delete end delKeys:%d expireKeys:%d zsetKeys:%d cost:%.3fs",
		jobId, delKeyNum,
		bdb.delExpireKeys.Load(),
		bdb.delExpireZsetKeys.Load(),
		time.Now().Sub(start).Seconds())
}
