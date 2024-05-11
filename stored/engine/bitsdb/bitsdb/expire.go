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
	"encoding/binary"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv/kv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"

	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
)

type expirePoolArgs struct {
	expireKey  []byte
	expireTime uint64
	version    uint64
	kind       uint8
	key        []byte
	dt         btools.DataType
	wg         *sync.WaitGroup
}

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
	if !bdb.isDelExpireRun.CompareAndSwap(0, 1) {
		log.Infof("[DELEXPIRE %d] ScanDelExpire is running, do nothing", jobId)
		return
	}

	logTag := fmt.Sprintf("[DELEXPIRE %d] scan delete expireDb", jobId)
	log.Infof("%s start", logTag)

	start := time.Now()
	delKeyNum := 0
	delKeyThreshold := base.DeleteMixKeyMaxNum

	defer func() {
		bdb.isDelExpireRun.Store(0)

		if r := recover(); r != nil {
			log.Errorf("%s panic err:%s stack:%s", logTag, r, string(debug.Stack()))
			return
		}

		log.Infof("%s end delKeys:%d expireDbKeys:%d metaDbKeys:%d prefixDeleteKeys:%d zsetDataDbKeys:%d zsetIndexDbKeys:%d cost:%.3fs",
			logTag,
			delKeyNum,
			bdb.delExpireStat.expireDbKeys.Load(),
			bdb.delExpireStat.metaDbKeys.Load(),
			bdb.delExpireStat.prefixDeleteKeys.Load(),
			bdb.delExpireStat.zsetDataDbKeys.Load(),
			bdb.delExpireStat.zsetIndexDbKeys.Load(),
			time.Now().Sub(start).Seconds())
	}()

	if !bdb.IsReady() || bdb.IsCheckpointHighPriority() {
		return
	}

	bdb.CheckpointExpireLock(true)
	defer bdb.CheckpointExpireLock(false)

	bdb.delExpireStat.expireDbKeys.Store(0)
	bdb.delExpireStat.metaDbKeys.Store(0)
	bdb.delExpireStat.prefixDeleteKeys.Store(0)
	bdb.delExpireStat.zsetDataDbKeys.Store(0)
	bdb.delExpireStat.zsetIndexDbKeys.Store(0)

	var nowTimeBuf [8]byte
	nowTime := uint64(tclock.GetTimestampMilli())
	binary.BigEndian.PutUint64(nowTimeBuf[:], nowTime+1)
	iterOpts := &bitskv.IterOptions{
		IsAll:      true,
		UpperBound: nowTimeBuf[:],
	}
	it := bdb.baseDb.DB.NewIteratorExpire(iterOpts)
	defer it.Close()

	wg := &sync.WaitGroup{}
	for it.First(); it.Valid(); it.Next() {
		if !bdb.IsReady() || bdb.IsCheckpointHighPriority() {
			break
		}

		iterKey := it.Key()
		timestamp, dt, keyVersion, keyKind, key, err := base.DecodeExpireKey(iterKey)
		if err != nil {
			log.Errorf("%s decode expireKey fail key:%s err:%s", logTag, string(iterKey), err)
			continue
		}
		if dt == btools.STRING {
			continue
		}

		if timestamp > nowTime || delKeyNum >= delKeyThreshold {
			break
		}

		delKeyNum++
		wg.Add(1)
		ep := &expirePoolArgs{
			expireKey:  iterKey,
			expireTime: timestamp,
			version:    keyVersion,
			kind:       keyKind,
			key:        key,
			dt:         dt,
			wg:         wg,
		}
		_ = bdb.baseDb.DelExpirePool.Invoke(ep)
	}
	wg.Wait()
}

func (bdb *BitsDB) deleteExpireDataFunc(args interface{}) {
	ep, ok := args.(*expirePoolArgs)
	if !ok {
		return
	}

	defer func() {
		ep.wg.Done()
		if r := recover(); r != nil {
			log.Errorf("deleteExpireDataFunc panic dt:%s err:%v stack:%s", ep.dt, r, string(debug.Stack()))
		}
	}()

	expireKey := ep.expireKey
	expireTime := ep.expireTime
	keyVersion := ep.version
	keyKind := ep.kind
	dataType := ep.dt
	key := ep.key
	keyHash := hash.Fnv32(key)

	var retErr error
	defer func() {
		if retErr != nil {
			log.Errorf("deleteExpireDataFunc fail dt:%s key:%s err:%s", dataType, unsafe2.String(key), retErr)
		}
	}()

	if dataType == btools.ZSET {
		var finished bool
		var zetDelCnt uint64
		finished, zetDelCnt, retErr = bdb.ZsetObj.DeleteZsetKeyByExpire(keyVersion, keyKind, keyHash)
		if retErr != nil {
			return
		}

		bdb.delExpireStat.zsetDataDbKeys.Add(zetDelCnt)
		bdb.delExpireStat.zsetIndexDbKeys.Add(zetDelCnt)

		if !finished {
			return
		}
	} else {
		switch dataType {
		case btools.SET:
			retErr = bdb.SetObj.DeleteDataKeyByExpire(keyVersion, keyHash)
		case btools.LIST:
			retErr = bdb.ListObj.DeleteDataKeyByExpire(keyVersion, keyHash)
		case btools.HASH:
			retErr = bdb.HashObj.DeleteDataKeyByExpire(keyVersion, keyHash)
		default:
			retErr = errors.New("not support dataType")
		}
		if retErr != nil {
			return
		}

		bdb.delExpireStat.prefixDeleteKeys.Add(1)
	}

	isDelMetaKey, err := bdb.baseDb.DeleteMetaKeyByExpire(dataType, key, keyHash, keyVersion, expireTime)
	if err != nil && !errors.Is(err, errn.ErrWrongType) {
		log.Errorf("delete metaKey fail dt:%s key:%s err:%s", dataType, unsafe2.String(key), err)
	}

	if err = bdb.baseDb.DeleteExpireKey(expireKey); err != nil {
		log.Errorf("delete expireKey fail dt:%s key:%s err:%s", dataType, unsafe2.String(key), err)
	}

	bdb.delExpireStat.expireDbKeys.Add(1)
	if isDelMetaKey {
		bdb.delExpireStat.metaDbKeys.Add(1)
	}
}
