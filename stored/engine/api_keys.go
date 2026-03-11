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
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"
)

func (b *Bitalos) Exists(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.Exists(key, khash)
}

func (b *Bitalos) Type(key []byte, khash uint32) (string, error) {
	return b.bitsdb.Type(key, khash)
}

func (b *Bitalos) TTL(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.PTTL(key, khash, false)
}

func (b *Bitalos) PTTL(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.PTTL(key, khash, true)
}

func (b *Bitalos) Persist(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.Persist(key, khash)
}

func (b *Bitalos) Expire(key []byte, khash uint32, duration int64) (int64, error) {
	if duration <= 0 {
		return b.bitsdb.Del(khash, key)
	}

	when := tclock.GetTimestampSecond() + duration
	return b.bitsdb.PExpireAt(key, khash, tclock.SetTimestampMilli(when))
}

func (b *Bitalos) ExpireAt(key []byte, khash uint32, when int64) (int64, error) {
	if when <= tclock.GetTimestampSecond() {
		return b.bitsdb.Del(khash, key)
	}

	return b.bitsdb.PExpireAt(key, khash, tclock.SetTimestampMilli(when))
}

func (b *Bitalos) PExpire(key []byte, khash uint32, duration int64) (int64, error) {
	if duration <= 0 {
		return b.bitsdb.Del(khash, key)
	}

	when := tclock.GetTimestampMilli() + duration
	return b.bitsdb.PExpireAt(key, khash, when)
}

func (b *Bitalos) PExpireAt(key []byte, khash uint32, when int64) (int64, error) {
	if when <= tclock.GetTimestampMilli() {
		return b.bitsdb.Del(khash, key)
	}

	return b.bitsdb.PExpireAt(key, khash, when)
}

func (b *Bitalos) Del(khash uint32, keys ...[]byte) (int64, error) {
	return b.bitsdb.Del(khash, keys...)
}
