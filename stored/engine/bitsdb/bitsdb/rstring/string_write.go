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

package rstring

import (
	"math"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"

	"github.com/zuoyebang/bitalostored/butils/hash"
)

func (so *StringObject) Incr(key []byte, khash uint32) (int64, error) {
	return so.incr(key, khash, 1)
}

func (so *StringObject) IncrBy(key []byte, khash uint32, increment int64) (int64, error) {
	return so.incr(key, khash, increment)
}

func (so *StringObject) IncrByFloat(key []byte, khash uint32, increment float64) (float64, error) {
	return so.incrFloat(key, khash, increment)
}

func (so *StringObject) Decr(key []byte, khash uint32) (int64, error) {
	return so.incr(key, khash, -1)
}

func (so *StringObject) DecrBy(key []byte, khash uint32, decrement int64) (int64, error) {
	return so.incr(key, khash, -decrement)
}

func (so *StringObject) GetSet(key []byte, khash uint32, value []byte) ([]byte, func(), error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, nil, err
	} else if err := btools.CheckValueSize(value); err != nil {
		return nil, nil, err
	}

	unlockKey := so.LockKey(khash)
	defer unlockKey()

	ek, ekCloser := base.EncodeMetaKey(key, khash)
	defer ekCloser()
	oldValue, _, getCloser, err := so.getValueCheckAliveForString(ek)
	if err != nil {
		return nil, nil, err
	}

	return oldValue, getCloser, so.setValueForString(ek, value, 0)
}

func (so *StringObject) MSet(khash uint32, args ...btools.KVPair) (err error) {
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
		if err = so.Set(args[i].Key, khash, args[i].Value); err != nil {
			break
		}
	}

	return err
}

func (so *StringObject) Set(key []byte, khash uint32, value []byte) error {
	if err := btools.CheckKeySize(key); err != nil {
		return err
	} else if err := btools.CheckValueSize(value); err != nil {
		return err
	}

	unlockKey := so.LockKey(khash)
	defer unlockKey()

	ek, ekcloser := base.EncodeMetaKey(key, khash)
	defer ekcloser()

	return so.setValueForString(ek, value, 0)
}

func (so *StringObject) SetNX(key []byte, khash uint32, value []byte) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	} else if err = btools.CheckValueSize(value); err != nil {
		return 0, err
	}

	unlockKey := so.LockKey(khash)
	defer unlockKey()

	ek, ekCloser := base.EncodeMetaKey(key, khash)
	v, _, vCloser, err := so.getValueCheckAliveForString(ek)
	defer func() {
		ekCloser()
		if vCloser != nil {
			vCloser()
		}
	}()
	if err != nil || v != nil {
		return 0, err
	}

	return 1, so.setValueForString(ek, value, 0)
}

func (so *StringObject) SetEX(key []byte, khash uint32, duration int64, value []byte, p bool) error {
	if err := btools.CheckKeySize(key); err != nil {
		return err
	} else if err = btools.CheckValueSize(value); err != nil {
		return err
	} else if duration <= 0 {
		return errn.ErrExpireValue
	}

	unlockKey := so.LockKey(khash)
	defer unlockKey()

	ek, ekCloser := base.EncodeMetaKey(key, khash)
	defer ekCloser()

	var timestamp uint64
	if p {
		timestamp = uint64(tclock.GetTimestampMilli() + duration)
	} else {
		timestamp = uint64(tclock.SetExpireAtMilli(duration))
	}

	if err := so.setValueForString(ek, value, timestamp); err != nil {
		return err
	}

	return nil
}

func (so *StringObject) SetNXEX(key []byte, khash uint32, duration int64, value []byte, p bool) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	} else if err = btools.CheckValueSize(value); err != nil {
		return 0, err
	} else if duration <= 0 || duration > math.MaxUint32 {
		return 0, errn.ErrExpireValue
	}

	unlockKey := so.LockKey(khash)
	defer unlockKey()

	ek, ekCloser := base.EncodeMetaKey(key, khash)
	val, _, valCloser, err := so.getValueCheckAliveForString(ek)
	defer func() {
		ekCloser()
		if valCloser != nil {
			valCloser()
		}
	}()
	if err != nil || val != nil {
		return 0, err
	}

	var newTtl uint64
	if p {
		newTtl = uint64(tclock.GetTimestampMilli() + duration)
	} else {
		newTtl = uint64(tclock.SetExpireAtMilli(duration))
	}

	if err = so.setValueForString(ek, value, newTtl); err != nil {
		return 0, err
	}

	return 1, nil
}

func (so *StringObject) SetRange(key []byte, khash uint32, offset int, value []byte) (int64, error) {
	if len(value) == 0 {
		return 0, nil
	}

	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	} else if len(value)+offset > btools.MaxValueSize {
		return 0, errn.ErrValueSize
	}

	unlockKey := so.LockKey(khash)
	defer unlockKey()

	ek, ekCloser := base.EncodeMetaKey(key, khash)
	oldValue, timestamp, oldValueCloser, err := so.getValueCheckAliveForString(ek)
	defer func() {
		ekCloser()
		if oldValueCloser != nil {
			oldValueCloser()
		}
	}()
	if err != nil {
		return 0, err
	}

	extra := offset + len(value) - len(oldValue)
	if extra > 0 {
		oldValue = append(oldValue, make([]byte, extra)...)
	}
	copy(oldValue[offset:], value)

	if err = so.setValueForString(ek, oldValue, timestamp); err != nil {
		return 0, err
	}

	return int64(len(oldValue)), nil
}

func (so *StringObject) Append(key []byte, khash uint32, value []byte) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}
	if len(value) == 0 {
		return 0, nil
	}

	unlockKey := so.LockKey(khash)
	defer unlockKey()

	ek, ekCloser := base.EncodeMetaKey(key, khash)
	oldValue, timestamp, oldValueCloser, err := so.getValueCheckAliveForString(ek)
	defer func() {
		ekCloser()
		if oldValueCloser != nil {
			oldValueCloser()
		}
	}()
	if err != nil {
		return 0, err
	}

	if len(oldValue)+len(value) > btools.MaxValueSize {
		return 0, errn.ErrValueSize
	}

	valueLen := len(oldValue) + len(value)
	if err = so.setMultiValueForString(ek, oldValue, value, timestamp); err != nil {
		return 0, nil
	}

	return int64(valueLen), nil
}
