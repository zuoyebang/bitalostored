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

func (b *Bitalos) LPush(key []byte, khash uint32, args ...[]byte) (int64, error) {
	return b.bitsdb.ListPush(key, khash, true, false, args...)
}

func (b *Bitalos) RPush(key []byte, khash uint32, args ...[]byte) (int64, error) {
	return b.bitsdb.ListPush(key, khash, false, false, args...)
}

func (b *Bitalos) LPushX(key []byte, khash uint32, args ...[]byte) (int64, error) {
	return b.bitsdb.ListPush(key, khash, false, true, args...)
}

func (b *Bitalos) RPushX(key []byte, khash uint32, args ...[]byte) (int64, error) {
	return b.bitsdb.ListPush(key, khash, true, true, args...)
}

func (b *Bitalos) LSet(key []byte, khash uint32, index int64, value []byte) error {
	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	return b.bitsdb.DB.ListSet(key, btools.GetSlotId(khash), index, value)
}

func (b *Bitalos) LPop(key []byte, khash uint32) ([]byte, func(), error) {
	return b.bitsdb.ListPop(key, khash, true)
}

func (b *Bitalos) RPop(key []byte, khash uint32) ([]byte, func(), error) {
	return b.bitsdb.ListPop(key, khash, false)
}

func (b *Bitalos) LTrim(key []byte, khash uint32, start, stop int64) error {
	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()
	return b.bitsdb.DB.ListTrim(key, btools.GetSlotId(khash), start, stop)
}

func (b *Bitalos) LClear(khash uint32, key ...[]byte) (int64, error) {
	return b.bitsdb.Del(khash, key...)
}

func (b *Bitalos) LLen(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.Length(key, khash, btools.DataTypeList)
}

func (b *Bitalos) LIndex(key []byte, khash uint32, index int64) ([]byte, func(), error) {
	return b.bitsdb.DB.ListIndex(key, btools.GetSlotId(khash), index)
}

func (b *Bitalos) LRange(key []byte, khash uint32, start, stop int64) ([][]byte, func(), error) {
	return b.bitsdb.DB.ListRange(key, btools.GetSlotId(khash), start, stop)
}
