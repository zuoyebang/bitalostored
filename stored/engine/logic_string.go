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

func (b *Bitalos) Append(key []byte, khash uint32, value []byte) (int64, error) {
	return b.bitsdb.StringObj.Append(key, khash, value)
}

func (b *Bitalos) BitCount(key []byte, khash uint32, start int, end int) (int64, error) {
	return b.bitsdb.StringObj.BitCount(key, khash, start, end)
}

func (b *Bitalos) BitPos(key []byte, khash uint32, on int, start int, end int) (int64, error) {
	return b.bitsdb.StringObj.BitPos(key, khash, on, start, end)
}

func (b *Bitalos) Decr(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.StringObj.Decr(key, khash)
}

func (b *Bitalos) DecrBy(key []byte, khash uint32, decrement int64) (int64, error) {
	return b.bitsdb.StringObj.DecrBy(key, khash, decrement)
}

func (b *Bitalos) Get(key []byte, khash uint32) ([]byte, func(), error) {
	return b.bitsdb.StringObj.Get(key, khash)
}

func (b *Bitalos) GetBit(key []byte, khash uint32, offset int) (int64, error) {
	return b.bitsdb.StringObj.GetBit(key, khash, offset)
}

func (b *Bitalos) GetRange(key []byte, khash uint32, start int, end int) ([]byte, func(), error) {
	return b.bitsdb.StringObj.GetRange(key, khash, start, end)
}

func (b *Bitalos) GetSet(key []byte, khash uint32, value []byte) ([]byte, func(), error) {
	return b.bitsdb.StringObj.GetSet(key, khash, value)
}

func (b *Bitalos) Incr(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.StringObj.Incr(key, khash)
}

func (b *Bitalos) IncrBy(key []byte, khash uint32, increment int64) (int64, error) {
	return b.bitsdb.StringObj.IncrBy(key, khash, increment)
}

func (b *Bitalos) IncrByFloat(key []byte, khash uint32, increment float64) (float64, error) {
	return b.bitsdb.StringObj.IncrByFloat(key, khash, increment)
}

func (b *Bitalos) MGet(khash uint32, keys ...[]byte) ([][]byte, []func(), error) {
	return b.bitsdb.StringObj.MGet(khash, keys...)
}

func (b *Bitalos) MSet(khash uint32, args ...btools.KVPair) error {
	return b.bitsdb.StringObj.MSet(khash, args...)
}

func (b *Bitalos) Set(key []byte, khash uint32, value []byte) error {
	return b.bitsdb.StringObj.Set(key, khash, value)
}

func (b *Bitalos) SetBit(key []byte, khash uint32, offset int, on int) (int64, error) {
	return b.bitsdb.StringObj.SetBit(key, khash, offset, on)
}

func (b *Bitalos) SetEX(key []byte, khash uint32, duration int64, value []byte) error {
	return b.bitsdb.StringObj.SetEX(key, khash, duration, value, false)
}

func (b *Bitalos) PSetEX(key []byte, khash uint32, duration int64, value []byte) error {
	return b.bitsdb.StringObj.SetEX(key, khash, duration, value, true)
}

func (b *Bitalos) SetNX(key []byte, khash uint32, value []byte) (int64, error) {
	return b.bitsdb.StringObj.SetNX(key, khash, value)
}

func (b *Bitalos) SetNXEX(key []byte, khash uint32, duration int64, value []byte) (int64, error) {
	return b.bitsdb.StringObj.SetNXEX(key, khash, duration, value, false)
}

func (b *Bitalos) PSetNXEX(key []byte, khash uint32, duration int64, value []byte) (int64, error) {
	return b.bitsdb.StringObj.SetNXEX(key, khash, duration, value, true)
}

func (b *Bitalos) SetRange(key []byte, khash uint32, offset int, value []byte) (int64, error) {
	return b.bitsdb.StringObj.SetRange(key, khash, offset, value)
}

func (b *Bitalos) StrLen(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.StringObj.StrLen(key, khash)
}
