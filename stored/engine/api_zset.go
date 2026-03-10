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

func (b *Bitalos) ZAdd(key []byte, khash uint32, args ...[]byte) (int64, error) {
	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	return b.bitsdb.DB.ZAdd(key, btools.GetSlotId(khash), args...)
}

func (b *Bitalos) ZIncrBy(
	key []byte, khash uint32, increment float64, member []byte,
) (float64, error) {
	if err := btools.CheckKeyFieldSize(key, member); err != nil {
		return 0, err
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	n, err := b.bitsdb.DB.ZIncrBy(key, btools.GetSlotId(khash), member, increment)
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (b *Bitalos) ZRem(key []byte, khash uint32, members ...[]byte) (int64, error) {
	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	return b.bitsdb.DB.ZRem(key, btools.GetSlotId(khash), members...)
}

func (b *Bitalos) ZRemRangeByScore(
	key []byte, khash uint32,
	min float64, max float64,
	leftClose bool, rightClose bool,
) (int64, error) {
	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	return b.bitsdb.DB.ZRemRangeByScore(key, btools.GetSlotId(khash), min, max, leftClose, rightClose)
}

func (b *Bitalos) ZRemRangeByRank(
	key []byte, khash uint32, start int64, stop int64,
) (int64, error) {
	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	return b.bitsdb.DB.ZRemRangeByRank(key, btools.GetSlotId(khash), start, stop)
}

func (b *Bitalos) ZRemRangeByLex(
	key []byte, khash uint32,
	min []byte, max []byte,
	leftClose bool, rightClose bool,
) (int64, error) {
	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	return b.bitsdb.DB.ZRemRangeByLex(key, btools.GetSlotId(khash), min, max, leftClose, rightClose)
}

func (b *Bitalos) ZRange(key []byte, khash uint32, start int64, stop int64) ([]btools.ScorePair, func(), error) {
	return b.bitsdb.DB.ZRange(key, btools.GetSlotId(khash), start, stop, false)
}

func (b *Bitalos) ZRevRange(key []byte, khash uint32, start int64, stop int64) ([]btools.ScorePair, func(), error) {
	return b.bitsdb.DB.ZRange(key, btools.GetSlotId(khash), start, stop, true)
}

func (b *Bitalos) ZRangeByScore(
	key []byte, khash uint32, min float64, max float64,
	leftClose bool, rightClose bool, offset int, count int,
) ([]btools.ScorePair, func(), error) {
	return b.bitsdb.DB.ZRangeByScore(key, btools.GetSlotId(khash), min, max, leftClose, rightClose, offset, count)
}

func (b *Bitalos) ZRevRangeByScore(
	key []byte, khash uint32, min float64, max float64,
	leftClose bool, rightClose bool, offset int, count int,
) ([]btools.ScorePair, func(), error) {
	return b.bitsdb.DB.ZRevRangeByScore(key, btools.GetSlotId(khash), min, max, leftClose, rightClose, offset, count)
}

func (b *Bitalos) ZRank(key []byte, khash uint32, member []byte) (int64, error) {
	if err := btools.CheckKeyFieldSize(key, member); err != nil {
		return 0, err
	}

	return b.bitsdb.DB.ZRank(key, btools.GetSlotId(khash), member, false)
}

func (b *Bitalos) ZRevRank(key []byte, khash uint32, member []byte) (int64, error) {
	if err := btools.CheckKeyFieldSize(key, member); err != nil {
		return 0, err
	}

	return b.bitsdb.DB.ZRank(key, btools.GetSlotId(khash), member, true)
}

func (b *Bitalos) ZRangeByLex(
	key []byte, khash uint32, min []byte, max []byte,
	leftClose bool, rightClose bool, offset int, count int,
) ([][]byte, func(), error) {
	return b.bitsdb.DB.ZRangeByLex(key, btools.GetSlotId(khash), min, max, leftClose, rightClose, offset, count)
}

func (b *Bitalos) ZLexCount(
	key []byte, khash uint32,
	min []byte, max []byte,
	leftClose bool, rightClose bool,
) (int64, error) {
	return b.bitsdb.DB.ZLexCount(key, btools.GetSlotId(khash), min, max, leftClose, rightClose)
}

func (b *Bitalos) ZCount(
	key []byte, khash uint32,
	min float64, max float64,
	leftClose bool, rightClose bool,
) (int64, error) {
	return b.bitsdb.DB.ZCount(key, btools.GetSlotId(khash), min, max, leftClose, rightClose)
}

func (b *Bitalos) ZScore(key []byte, khash uint32, member []byte) (float64, error) {
	if err := btools.CheckKeyFieldSize(key, member); err != nil {
		return 0, err
	}

	score, err := b.bitsdb.DB.ZScore(key, btools.GetSlotId(khash), member)
	if err != nil {
		return 0, btools.ErrZsetMemberNil
	}
	return score, nil
}

func (b *Bitalos) ZClear(khash uint32, key ...[]byte) (int64, error) {
	return b.bitsdb.Del(khash, key...)
}

func (b *Bitalos) ZCard(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.Length(key, khash, btools.DataTypeZset)
}
