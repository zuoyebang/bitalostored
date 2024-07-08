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

import "github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"

func (b *Bitalos) ZAdd(
	key []byte, khash uint32, args ...btools.ScorePair,
) (int64, error) {
	return b.bitsdb.ZsetObj.ZAdd(key, khash, false, args...)
}

func (b *Bitalos) ZIncrBy(
	key []byte, khash uint32, delta float64, member []byte,
) (float64, error) {
	return b.bitsdb.ZsetObj.ZIncrBy(key, khash, false, delta, member)
}

func (b *Bitalos) ZRem(
	key []byte, khash uint32, members ...[]byte,
) (int64, error) {
	return b.bitsdb.ZsetObj.ZRem(key, khash, members...)
}

func (b *Bitalos) ZRemRangeByScore(
	key []byte, khash uint32,
	min float64, max float64,
	leftClose bool, rightClose bool,
) (int64, error) {
	return b.bitsdb.ZsetObj.ZRemRangeByScore(key, khash, min, max, leftClose, rightClose)
}

func (b *Bitalos) ZRemRangeByRank(
	key []byte, khash uint32, start int64, stop int64,
) (int64, error) {
	return b.bitsdb.ZsetObj.ZRemRangeByRank(key, khash, start, stop)
}

func (b *Bitalos) ZRemRangeByLex(
	key []byte, khash uint32,
	min []byte, max []byte,
	leftClose bool, rightClose bool,
) (int64, error) {
	return b.bitsdb.ZsetObj.ZRemRangeByLex(key, khash, min, max, leftClose, rightClose)
}

func (b *Bitalos) ZRangeByLex(
	key []byte, khash uint32,
	min []byte, max []byte,
	leftClose bool, rightClose bool,
	offset int, count int,
) ([][]byte, error) {
	return b.bitsdb.ZsetObj.ZRangeByLex(key, khash, min, max, leftClose, rightClose, offset, count)
}

func (b *Bitalos) ZRangeGeneric(
	key []byte, khash uint32, start int64, stop int64, reverse bool,
) ([]btools.ScorePair, error) {
	if reverse {
		return b.bitsdb.ZsetObj.ZRevRange(key, khash, start, stop)
	} else {
		return b.bitsdb.ZsetObj.ZRange(key, khash, start, stop)
	}
}

func (b *Bitalos) ZRangeByScoreGeneric(
	key []byte, khash uint32,
	min float64, max float64,
	leftClose bool, rightClose bool,
	offset int, count int, reverse bool,
) ([]btools.ScorePair, error) {
	if reverse {
		return b.bitsdb.ZsetObj.ZRevRangeByScore(key, khash, min, max, leftClose, rightClose, offset, count)
	} else {
		return b.bitsdb.ZsetObj.ZRangeByScore(key, khash, min, max, leftClose, rightClose, offset, count)
	}
}

func (b *Bitalos) ZRank(key []byte, khash uint32, member []byte) (int64, error) {
	return b.bitsdb.ZsetObj.ZRank(key, khash, member)
}

func (b *Bitalos) ZRevRank(key []byte, khash uint32, member []byte) (int64, error) {
	return b.bitsdb.ZsetObj.ZRevRank(key, khash, member)
}

func (b *Bitalos) ZScore(key []byte, khash uint32, member []byte) (float64, error) {
	return b.bitsdb.ZsetObj.ZScore(key, khash, member)
}

func (b *Bitalos) ZLexCount(
	key []byte, khash uint32,
	min []byte, max []byte,
	leftClose bool, rightClose bool,
) (int64, error) {
	return b.bitsdb.ZsetObj.ZLexCount(key, khash, min, max, leftClose, rightClose)
}

func (b *Bitalos) ZCount(
	key []byte, khash uint32,
	min float64, max float64,
	leftClose bool, rightClose bool,
) (int64, error) {
	return b.bitsdb.ZsetObj.ZCount(key, khash, min, max, leftClose, rightClose)
}

func (b *Bitalos) ZClear(khash uint32, key ...[]byte) (int64, error) {
	return b.bitsdb.ZsetObj.Del(khash, key...)
}

func (b *Bitalos) ZCard(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.ZsetObj.ZCard(key, khash)
}
