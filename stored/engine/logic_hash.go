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

func (b *Bitalos) HClear(khash uint32, key ...[]byte) (int64, error) {
	return b.bitsdb.HashObj.Del(khash, key...)
}

func (b *Bitalos) HDel(key []byte, khash uint32, args ...[]byte) (int64, error) {
	return b.bitsdb.HashObj.HDel(key, khash, args...)
}

func (b *Bitalos) HGet(key []byte, khash uint32, field []byte) ([]byte, func(), error) {
	return b.bitsdb.HashObj.HGet(key, khash, field)
}

func (b *Bitalos) HGetAll(key []byte, khash uint32) ([]btools.FVPair, []func(), error) {
	return b.bitsdb.HashObj.HGetAll(key, khash)
}

func (b *Bitalos) HIncrBy(key []byte, khash uint32, field []byte, delta int64) (int64, error) {
	return b.bitsdb.HashObj.HIncrBy(key, khash, field, delta)
}

func (b *Bitalos) HKeys(key []byte, khash uint32) ([][]byte, []func(), error) {
	return b.bitsdb.HashObj.HKeys(key, khash)
}

func (b *Bitalos) HLen(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.HashObj.HLen(key, khash)
}

func (b *Bitalos) HMget(key []byte, khash uint32, args ...[]byte) ([][]byte, []func(), error) {
	return b.bitsdb.HashObj.HMget(key, khash, args...)
}

func (b *Bitalos) HMset(key []byte, khash uint32, args ...btools.FVPair) error {
	return b.bitsdb.HashObj.HMset(key, khash, args...)
}

func (b *Bitalos) HSet(key []byte, khash uint32, field []byte, value []byte) (int64, error) {
	return b.bitsdb.HashObj.HSet(key, khash, field, value)
}

func (b *Bitalos) HValues(key []byte, khash uint32) ([][]byte, []func(), error) {
	return b.bitsdb.HashObj.HValues(key, khash)
}
