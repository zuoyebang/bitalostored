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

func (b *Bitalos) SAdd(key []byte, khash uint32, members ...[]byte) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	n, err := b.bitsdb.DB.SAdd(key, btools.GetSlotId(khash), members...)
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (b *Bitalos) SAddX(key []byte, khash uint32, isExist bool, members ...[]byte) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	n, err := b.bitsdb.DB.SAddX(key, btools.GetSlotId(khash), isExist, members...)
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (b *Bitalos) SRem(key []byte, khash uint32, fields ...[]byte) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	return b.bitsdb.DB.SRem(key, btools.GetSlotId(khash), fields...)
}

func (b *Bitalos) SRemX(key []byte, khash uint32, fields ...[]byte) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	return b.bitsdb.DB.SRemX(key, btools.GetSlotId(khash), fields...)
}

func (b *Bitalos) SPop(key []byte, khash uint32, count int64) ([][]byte, func(), error) {
	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	slotId := btools.GetSlotId(khash)
	return b.bitsdb.DB.SPop(key, slotId, count)
}

func (b *Bitalos) SCard(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.Length(key, khash, btools.DataTypeSet)
}

func (b *Bitalos) SClear(khash uint32, key ...[]byte) (int64, error) {
	return b.bitsdb.Del(khash, key...)
}

func (b *Bitalos) SIsMember(key []byte, khash uint32, member []byte) (int64, error) {
	return b.bitsdb.DB.SIsMember(key, btools.GetSlotId(khash), member)
}

func (b *Bitalos) SMembers(key []byte, khash uint32) ([][]byte, func(), error) {
	return b.bitsdb.DB.SMembers(key, btools.GetSlotId(khash))
}

func (b *Bitalos) SRandMember(key []byte, khash uint32, count int64) ([][]byte, func(), error) {
	return b.bitsdb.DB.SRandMember(key, btools.GetSlotId(khash), count)
}
