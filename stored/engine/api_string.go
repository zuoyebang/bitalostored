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
	"math"
	"strconv"

	"github.com/zuoyebang/bitalostored/stored/engine/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/shopspring/decimal"
)

func (b *Bitalos) Get(key []byte, khash uint32) ([]byte, func(), error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, nil, err
	}

	value, _, closer, err := b.getStringAlive(key, btools.GetSlotId(khash))
	return value, closer, err
}

func (b *Bitalos) GetRange(key []byte, khash uint32, start int, end int) ([]byte, func(), error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, nil, err
	}

	kslotid := btools.GetSlotId(khash)

	value, _, valCloser, err := b.getStringAlive(key, kslotid)
	if err != nil || value == nil {
		return []byte{}, valCloser, err
	}

	valLen := len(value)
	begin, stop, hasRange := getSliceRange(start, end, valLen)
	if !hasRange {
		return []byte{}, nil, nil
	}
	return value[begin : stop+1], valCloser, nil
}

func (b *Bitalos) StrLen(key []byte, khash uint32) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	t, ok, err := b.bitmapStrlen(key)
	if ok {
		return t, err
	}

	value, _, valueCloser, err := b.getStringAlive(key, btools.GetSlotId(khash))
	defer func() {
		if valueCloser != nil {
			valueCloser()
		}
	}()
	if err != nil {
		return 0, err
	}
	return int64(len(value)), nil
}

func (b *Bitalos) MGet(khash uint32, keys ...[]byte) ([][]byte, []func(), error) {
	keyNum := len(keys)
	kslotids := make([]uint16, keyNum)
	vals := make([][]byte, keyNum)
	valClosers := make([]func(), keyNum)

	var isHashTag bool
	firstKeyHash := hash.Fnv32(keys[0])
	if firstKeyHash != khash {
		isHashTag = true
		firstKeyHash = khash
	}

	for i, key := range keys {
		if i == 0 {
			khash = firstKeyHash
		} else if !isHashTag {
			khash = hash.Fnv32(key)
		}
		if err := btools.CheckKeySize(keys[i]); err == nil {
			kslotids[i] = btools.GetSlotId(khash)
		}
	}

	for i, kslotid := range kslotids {
		vals[i], _, valClosers[i], _ = b.getStringAlive(keys[i], kslotid)
	}

	return vals, valClosers, nil
}

func (b *Bitalos) GetSet(key []byte, khash uint32, value []byte) ([]byte, func(), error) {
	if err := btools.CheckKeyValueSize(key, value); err != nil {
		return nil, nil, err
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	kslotid := btools.GetSlotId(khash)

	oldValue, timestamp, getCloser, err := b.getStringAlive(key, kslotid)
	if err != nil {
		return nil, nil, err
	}

	ov := utils.CloneBytes(oldValue)
	return ov, getCloser, b.setStringValue(key, kslotid, timestamp, value)
}

func (b *Bitalos) Incr(key []byte, khash uint32) (int64, error) {
	return b.incr(key, khash, 1)
}

func (b *Bitalos) IncrBy(key []byte, khash uint32, increment int64) (int64, error) {
	return b.incr(key, khash, increment)
}

func (b *Bitalos) IncrByFloat(key []byte, khash uint32, increment float64) (float64, error) {
	return b.incrFloat(key, khash, increment)
}

func (b *Bitalos) Decr(key []byte, khash uint32) (int64, error) {
	return b.incr(key, khash, -1)
}

func (b *Bitalos) DecrBy(key []byte, khash uint32, decrement int64) (int64, error) {
	return b.incr(key, khash, -decrement)
}

func (b *Bitalos) MSet(khash uint32, args ...btools.KVPair) error {
	if len(args) == 0 {
		return nil
	}
	var isHashTag bool
	firstKeyHash := hash.Fnv32(args[0].Key)
	if firstKeyHash != khash {
		isHashTag = true
		firstKeyHash = khash
	}

	for i := 0; i < len(args); i++ {
		if i == 0 {
			khash = firstKeyHash
		} else if !isHashTag {
			khash = hash.Fnv32(args[i].Key)
		}
		if err := b.Set(args[i].Key, khash, args[i].Value); err != nil {
			return err
		}
	}

	return nil
}

func (b *Bitalos) Set(key []byte, khash uint32, value []byte) error {
	if err := btools.CheckKeyValueSize(key, value); err != nil {
		return err
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	return b.setStringValue(key, btools.GetSlotId(khash), 0, value)
}

func (b *Bitalos) PSetEX(key []byte, khash uint32, duration int64, value []byte) error {
	return b.setEX(key, khash, duration, value, true)
}

func (b *Bitalos) SetEX(key []byte, khash uint32, duration int64, value []byte) error {
	return b.setEX(key, khash, duration, value, false)
}

func (b *Bitalos) setEX(key []byte, khash uint32, duration int64, value []byte, p bool) error {
	if err := btools.CheckKeyValueSize(key, value); err != nil {
		return err
	} else if duration <= 0 {
		return errn.ErrExpireValue
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	var timestamp uint64
	if p {
		timestamp = uint64(tclock.GetTimestampMilli() + duration)
	} else {
		timestamp = uint64(tclock.SetExpireAtMilli(duration))
	}
	return b.setStringValue(key, btools.GetSlotId(khash), timestamp, value)
}

func (b *Bitalos) SetNX(key []byte, khash uint32, value []byte) (int64, error) {
	if err := btools.CheckKeyValueSize(key, value); err != nil {
		return 0, err
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	kslotid := btools.GetSlotId(khash)

	v, timestamp, vCloser, err := b.getStringAlive(key, kslotid)
	defer func() {
		if vCloser != nil {
			vCloser()
		}
	}()
	if err != nil || v != nil {
		return 0, err
	}

	return 1, b.setStringValue(key, kslotid, timestamp, value)
}

func (b *Bitalos) SetNXEX(key []byte, khash uint32, duration int64, value []byte) (int64, error) {
	return b.setNXEX(key, khash, duration, value, false)
}

func (b *Bitalos) PSetNXEX(key []byte, khash uint32, duration int64, value []byte) (int64, error) {
	return b.setNXEX(key, khash, duration, value, true)
}

func (b *Bitalos) setNXEX(key []byte, khash uint32, duration int64, value []byte, p bool) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	} else if err = btools.CheckValueSize(value); err != nil {
		return 0, err
	} else if duration <= 0 || duration > math.MaxUint32 {
		return 0, errn.ErrExpireValue
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	kslotid := btools.GetSlotId(khash)

	val, _, valCloser, err := b.getStringAlive(key, kslotid)
	defer func() {
		if valCloser != nil {
			valCloser()
		}
	}()
	if err != nil || val != nil {
		return 0, err
	}

	var timestamp uint64
	if p {
		timestamp = uint64(tclock.GetTimestampMilli() + duration)
	} else {
		timestamp = uint64(tclock.SetExpireAtMilli(duration))
	}

	if err = b.setStringValue(key, kslotid, timestamp, value); err != nil {
		return 0, err
	}

	return 1, nil
}

func (b *Bitalos) SetRange(key []byte, khash uint32, offset int, value []byte) (int64, error) {
	if len(value) == 0 {
		return 0, nil
	} else if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	} else if len(value)+offset > btools.MaxValueSize {
		return 0, errn.ErrValueSize
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	kslotid := btools.GetSlotId(khash)

	oldValue, timestamp, oldValueCloser, err := b.getStringAlive(key, kslotid)
	defer func() {
		if oldValueCloser != nil {
			oldValueCloser()
		}
	}()
	if err != nil {
		return 0, err
	}

	if len(oldValue) > 0 {
		oldValue = utils.CloneBytes(oldValue)
	}

	extra := offset + len(value) - len(oldValue)
	if extra > 0 {
		oldValue = append(oldValue, make([]byte, extra)...)
	}
	copy(oldValue[offset:], value)

	if err = b.setStringValue(key, kslotid, timestamp, oldValue); err != nil {
		return 0, err
	}

	return int64(len(oldValue)), nil
}

func (b *Bitalos) Append(key []byte, khash uint32, newValue []byte) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}
	if len(newValue) == 0 {
		return 0, nil
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	kslotid := btools.GetSlotId(khash)

	oldValue, timestamp, oldValueCloser, err := b.getStringAlive(key, kslotid)
	defer func() {
		if oldValueCloser != nil {
			oldValueCloser()
		}
	}()
	if err != nil {
		return 0, err
	}

	oldValueLen := len(oldValue)
	newValueLen := len(newValue)
	valueLen := oldValueLen + newValueLen
	if valueLen > btools.MaxValueSize {
		return 0, errn.ErrValueSize
	}

	if oldValueLen == 0 {
		if err = b.setStringValue(key, kslotid, timestamp, newValue); err != nil {
			return 0, nil
		}
	} else {
		value := make([]byte, valueLen)
		n := copy(value, oldValue)
		copy(value[n:], newValue)
		if err = b.setStringValue(key, kslotid, timestamp, value); err != nil {
			return 0, nil
		}
	}

	return int64(valueLen), nil
}

func (b *Bitalos) getStringAlive(key []byte, kslotid uint16) ([]byte, uint64, func(), error) {
	val, timestamp, closer, err := b.bitsdb.GetKV(key, kslotid)
	if val == nil || err != nil {
		return nil, 0, closer, err
	}

	if timestamp > 0 {
		nowTime := tclock.GetTimestampMilli()
		if int64(timestamp) <= nowTime {
			return nil, 0, closer, nil
		}
	}

	return val, timestamp, closer, nil
}

func (b *Bitalos) setStringValue(key []byte, kslotid uint16, timestamp uint64, value []byte) (err error) {
	return b.bitsdb.SetKV(key, kslotid, btools.DataTypeString, timestamp, value)
}

func (b *Bitalos) incr(key []byte, khash uint32, delta int64) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	var n int64
	kslotid := btools.GetSlotId(khash)

	val, timestamp, valCloser, err := b.getStringAlive(key, kslotid)
	defer func() {
		if valCloser != nil {
			valCloser()
		}
	}()
	n, err = btools.StrInt64(val, err)
	if err != nil {
		return 0, errn.ErrValue
	}

	n += delta
	value := extend.FormatInt64ToSlice(n)
	return n, b.setStringValue(key, kslotid, timestamp, value)
}

func (b *Bitalos) incrFloat(key []byte, khash uint32, delta float64) (float64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	var n float64
	kslotid := btools.GetSlotId(khash)
	val, timestamp, valCloser, err := b.getStringAlive(key, kslotid)
	defer func() {
		if valCloser != nil {
			valCloser()
		}
	}()
	n, err = btools.StrFloat64(val, err)
	if err != nil {
		return 0, err
	}

	f, _ := decimal.NewFromFloat(n).Add(decimal.NewFromFloat(delta)).Float64()
	value := []byte(strconv.FormatFloat(f, 'f', -1, 64))
	return f, b.setStringValue(key, kslotid, timestamp, value)
}

func getSliceRange(start int, end int, valLen int) (int, int, bool) {
	if start < 0 {
		start = valLen + start
	}

	if end < 0 {
		end = valLen + end
	}

	if start < 0 {
		start = 0
	}

	if end < 0 {
		end = 0
	}

	if end >= valLen {
		end = valLen - 1
	}

	if start >= valLen || start > end {
		return start, end, false
	}
	return start, end, true
}
