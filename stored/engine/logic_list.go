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

package engine

func (b *Bitalos) LClear(khash uint32, key ...[]byte) (int64, error) {
	return b.bitsdb.ListObj.Del(khash, key...)
}

func (b *Bitalos) LIndex(key []byte, khash uint32, index int64) ([]byte, func(), error) {
	return b.bitsdb.ListObj.LIndex(key, khash, index)
}

func (b *Bitalos) LLen(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.ListObj.LLen(key, khash)
}

func (b *Bitalos) LRem(key []byte, khash uint32, count int64, value []byte) (int64, error) {
	return b.bitsdb.ListObj.LRem(key, khash, count, value)
}

func (b *Bitalos) LInsert(key []byte, khash uint32, isbefore bool, pivot, value []byte) (int64, error) {
	return b.bitsdb.ListObj.LInsert(key, khash, isbefore, pivot, value)
}

func (b *Bitalos) LPop(key []byte, khash uint32) ([]byte, func(), error) {
	return b.bitsdb.ListObj.LPop(key, khash)
}

func (b *Bitalos) RPop(key []byte, khash uint32) ([]byte, func(), error) {
	return b.bitsdb.ListObj.RPop(key, khash)
}

func (b *Bitalos) LPush(key []byte, khash uint32, args ...[]byte) (int64, error) {
	return b.bitsdb.ListObj.LPush(key, khash, args...)
}

func (b *Bitalos) RPush(key []byte, khash uint32, args ...[]byte) (int64, error) {
	return b.bitsdb.ListObj.RPush(key, khash, args...)
}

func (b *Bitalos) LPushX(key []byte, khash uint32, args ...[]byte) (int64, error) {
	return b.bitsdb.ListObj.LPushX(key, khash, args...)
}

func (b *Bitalos) RPushX(key []byte, khash uint32, args ...[]byte) (int64, error) {
	return b.bitsdb.ListObj.RPushX(key, khash, args...)
}

func (b *Bitalos) LRange(key []byte, khash uint32, start int64, stop int64) ([][]byte, error) {
	return b.bitsdb.ListObj.LRange(key, khash, start, stop)
}

func (b *Bitalos) LSet(key []byte, khash uint32, index int64, value []byte) error {
	return b.bitsdb.ListObj.LSet(key, khash, index, value)
}

func (b *Bitalos) LTrim(key []byte, khash uint32, start, stop int64) error {
	return b.bitsdb.ListObj.LTrim(key, khash, start, stop)
}

func (b *Bitalos) LTrimBack(key []byte, khash uint32, trimSize int64) (int64, error) {
	return b.bitsdb.ListObj.LTrimBack(key, khash, trimSize)
}

func (b *Bitalos) LTrimFront(key []byte, khash uint32, trimSize int64) (int64, error) {
	return b.bitsdb.ListObj.LTrimFront(key, khash, trimSize)
}
