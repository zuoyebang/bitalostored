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
	"github.com/zuoyebang/bitalostored/stored/engine/btools"
)

func (b *Bitalos) HClear(khash uint32, key ...[]byte) (int64, error) {
	return b.bitsdb.Del(khash, key...)
}

func (b *Bitalos) HDel(key []byte, khash uint32, fields ...[]byte) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	return b.bitsdb.DB.HDel(key, btools.GetSlotId(khash), fields...)
}

func (b *Bitalos) HDelX(key []byte, khash uint32, fields ...[]byte) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	return b.bitsdb.DB.HDelX(key, btools.GetSlotId(khash), fields...)
}

func (b *Bitalos) HMset(key []byte, khash uint32, fields ...[]byte) error {
	if err := btools.CheckKeySize(key); err != nil {
		return err
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	_, err := b.bitsdb.DB.HMSet(key, btools.GetSlotId(khash), fields...)
	if err != nil {
		return err
	}
	return nil
}

func (b *Bitalos) HMsetX(key []byte, khash uint32, isExist bool, fields ...[]byte) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	return b.bitsdb.DB.HMSetX(key, btools.GetSlotId(khash), isExist, fields...)
}

func (b *Bitalos) HSet(key []byte, khash uint32, field []byte, value []byte) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	return b.bitsdb.DB.HMSet(key, btools.GetSlotId(khash), field, value)
}

func (b *Bitalos) HIncrBy(key []byte, khash uint32, field []byte, increment int64) (int64, error) {
	if err := btools.CheckKeyFieldSize(key, field); err != nil {
		return 0, err
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	n, err := b.bitsdb.DB.HIncrBy(key, btools.GetSlotId(khash), field, increment)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (b *Bitalos) HLen(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.Length(key, khash, btools.DataTypeHash)
}

func (b *Bitalos) HGet(key []byte, khash uint32, field []byte) ([]byte, func(), error) {
	if err := btools.CheckKeyFieldSize(key, field); err != nil {
		return nil, nil, err
	}

	value, closer, err := b.bitsdb.DB.HGet(key, btools.GetSlotId(khash), field)
	if err != nil {
		if b.bitsdb.IsNotFound(err) {
			if closer != nil {
				closer()
			}
			return nil, nil, nil
		}
	}
	return value, closer, err
}

func (b *Bitalos) HMget(key []byte, khash uint32, fields ...[]byte) ([][]byte, []func(), error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, nil, err
	}

	return b.bitsdb.DB.HMGet(key, btools.GetSlotId(khash), fields...)
}

func (b *Bitalos) HValues(key []byte, khash uint32) ([][]byte, func(), error) {
	return b.bitsdb.DB.HValues(key, btools.GetSlotId(khash))
}

func (b *Bitalos) HKeys(key []byte, khash uint32) ([][]byte, func(), error) {
	return b.bitsdb.DB.HKeys(key, btools.GetSlotId(khash))
}

func (b *Bitalos) HGetAll(key []byte, khash uint32) ([][]byte, func(), error) {
	slotId := btools.GetSlotId(khash)
	return b.bitsdb.DB.HGetAll(key, slotId)
}
