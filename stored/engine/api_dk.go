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

package engine

import (
	"fmt"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb"
	"github.com/zuoyebang/bitalostored/stored/engine/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/hash"
)

func (b *Bitalos) DKCreate(key []byte, khash uint32, dataType uint8, shardNum uint32) error {
	if err := btools.CheckKeySize(key); err != nil {
		return err
	}

	var dt uint8
	switch dataType {
	case btools.DataTypeHash:
		dt = btools.DataTypeDKHash
	case btools.DataTypeSet:
		dt = btools.DataTypeDKSet
	default:
		return errn.ErrDKType
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	return b.bitsdb.DB.DKCreate(key, btools.GetSlotId(khash), dt, shardNum)
}

func (b *Bitalos) DKCreateShard(dataType uint8, keys ...[]byte) error {
	if dataType != btools.DataTypeHash && dataType != btools.DataTypeSet {
		return errn.ErrDKShardType
	}

	type ckey struct {
		key    []byte
		slotId uint16
	}
	var createdKeys []ckey

	createShardKey := func(k []byte) error {
		khash := hash.Fnv32(k)
		slotId := btools.GetSlotId(khash)
		unlockKey := b.bitsdb.LockKey(khash)
		defer unlockKey()

		if e := b.bitsdb.DB.DKCreateShard(k, slotId, dataType); e != nil {
			return e
		}

		createdKeys = append(createdKeys, ckey{
			key:    k,
			slotId: slotId,
		})
		return nil
	}

	for i := range keys {
		if err := createShardKey(keys[i]); err != nil {
			if len(createdKeys) > 0 {
				for _, ck := range createdKeys {
					_, _, e := b.bitsdb.DB.Delete(ck.key, ck.slotId)
					if e != nil {
						log.Errorf("dk.createshard rollback delete key:%s fail err:%s", string(ck.key), err)
					}
				}
			}
			return err
		}
	}

	return nil
}

func (b *Bitalos) DKIncrBySize(key []byte, khash uint32, increment int64) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	n, err := b.bitsdb.DB.DKIncrBySize(key, btools.GetSlotId(khash), increment)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (b *Bitalos) DKInfo(key []byte, khash uint32) (uint8, uint32, uint64, int64) {
	alive, ttl, dt, _, _, size, _, shardNum, _ := b.bitsdb.GetMetaAlive(key, khash, btools.DataTypeNone)
	if !alive {
		return 0, 0, 0, bitsdb.ErrnoKeyNotFoundOrExpire
	}

	if dt == btools.DataTypeDKHash {
		dt = btools.DataTypeHash
	} else if dt == btools.DataTypeDKSet {
		dt = btools.DataTypeSet
	}

	ttl = tclock.SetTtlMilliToSec(ttl)
	return dt, shardNum, size, ttl
}

func (b *Bitalos) DKHSet(args ...[]byte) (res [][]byte, err error) {
	var n int64
	var pos, fieldNum, keyNum int
	argsNum := len(args)
	for pos+4 <= argsNum {
		keyNum++
		key := args[pos]
		fieldNum, err = utils.ByteToInt(args[pos+1])
		start := pos + 2
		end := start + fieldNum*2
		if err != nil {
			return nil, err
		} else if fieldNum == 0 {
			return nil, fmt.Errorf("field num is zero")
		} else if end > argsNum {
			return nil, fmt.Errorf("field num is not match")
		}

		n, err = b.HMsetX(key, hash.Fnv32(key), true, args[start:end]...)
		if err != nil {
			return nil, err
		}
		pos = end
		res = append(res, key, extend.FormatInt64ToSlice(n))
	}

	return res, nil
}

func (b *Bitalos) DKHDel(args ...[]byte) (res [][]byte, err error) {
	var n int64
	var pos, fieldNum, keyNum int
	argsNum := len(args)
	for pos+3 <= argsNum {
		keyNum++
		key := args[pos]
		fieldNum, err = utils.ByteToInt(args[pos+1])
		start := pos + 2
		end := start + fieldNum
		if err != nil {
			return nil, err
		} else if fieldNum == 0 {
			return nil, fmt.Errorf("field num is zero")
		} else if end > argsNum {
			return nil, fmt.Errorf("field num is not match")
		}

		n, _ = b.HDelX(key, hash.Fnv32(key), args[start:end]...)
		pos = end
		res = append(res, key, extend.FormatInt64ToSlice(n))
	}

	return res, nil
}

func (b *Bitalos) DKHMGet(args ...[]byte) (res [][]byte, vclosers []func(), err error) {
	var pos, fieldNum int
	argsNum := len(args)
	for pos+3 <= argsNum {
		key := args[pos]
		fieldNum, err = utils.ByteToInt(args[pos+1])
		start := pos + 2
		end := start + fieldNum
		if err != nil {
			for i := range vclosers {
				vclosers[i]()
			}
			return nil, nil, err
		} else if fieldNum == 0 || end > argsNum {
			for i := range vclosers {
				vclosers[i]()
			}
			return nil, nil, fmt.Errorf("field num zero or not match")
		}

		v, vcloser, _ := b.HMget(key, hash.Fnv32(key), args[start:end]...)
		pos = end
		vn := len(v)
		res = append(res, key, extend.FormatInt64ToSlice(int64(vn)))
		for i := range v {
			res = append(res, v[i])
		}
		if len(vcloser) > 0 {
			vclosers = append(vclosers, vcloser...)
		}
	}

	return res, vclosers, nil
}

func (b *Bitalos) DKSAdd(args ...[]byte) (res [][]byte, err error) {
	var n int64
	var pos, fieldNum, keyNum int
	argsNum := len(args)
	for pos+3 <= argsNum {
		keyNum++
		key := args[pos]
		fieldNum, err = utils.ByteToInt(args[pos+1])
		start := pos + 2
		end := start + fieldNum
		if err != nil {
			return nil, err
		} else if fieldNum == 0 {
			return nil, fmt.Errorf("field num is zero")
		} else if end > argsNum {
			return nil, fmt.Errorf("field num is not match")
		}

		n, err = b.SAddX(key, hash.Fnv32(key), true, args[start:end]...)
		if err != nil {
			return nil, err
		}
		pos = end
		res = append(res, key, extend.FormatInt64ToSlice(n))
	}

	return res, nil
}

func (b *Bitalos) DKSPop(key []byte, khash uint32, count int64) ([][]byte, func(), error) {
	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	slotId := btools.GetSlotId(khash)
	return b.bitsdb.DB.SPopX(key, slotId, count)
}

func (b *Bitalos) DKSRem(args ...[]byte) (res [][]byte, err error) {
	var n int64
	var pos, fieldNum, keyNum int
	argsNum := len(args)
	for pos+3 <= argsNum {
		keyNum++
		key := args[pos]
		fieldNum, err = utils.ByteToInt(args[pos+1])
		start := pos + 2
		end := start + fieldNum
		if err != nil {
			return nil, err
		} else if fieldNum == 0 {
			return nil, fmt.Errorf("field num is zero")
		} else if end > argsNum {
			return nil, fmt.Errorf("field num is not match")
		}
		n, _ = b.SRemX(key, hash.Fnv32(key), args[start:end]...)
		pos = end
		res = append(res, key, extend.FormatInt64ToSlice(n))
	}

	return res, nil
}
