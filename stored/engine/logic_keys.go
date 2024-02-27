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

func (b *Bitalos) Exists(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.StringObj.Exists(key, khash)
}

func (b *Bitalos) Type(key []byte, khash uint32) (string, error) {
	return b.bitsdb.StringObj.Type(key, khash)
}

func (b *Bitalos) TTl(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.StringObj.TTL(key, khash)
}

func (b *Bitalos) PTTl(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.StringObj.PTTL(key, khash)
}

func (b *Bitalos) Persist(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.StringObj.BasePersist(key, khash)
}

func (b *Bitalos) Expire(key []byte, khash uint32, duration int64) (int64, error) {
	return b.bitsdb.StringObj.Expire(key, khash, duration)
}

func (b *Bitalos) ExpireAt(key []byte, khash uint32, when int64) (int64, error) {
	return b.bitsdb.StringObj.ExpireAt(key, khash, when)
}

func (b *Bitalos) PExpire(key []byte, khash uint32, duration int64) (int64, error) {
	return b.bitsdb.StringObj.PExpire(key, khash, duration)
}

func (b *Bitalos) PExpireAt(key []byte, khash uint32, when int64) (int64, error) {
	return b.bitsdb.StringObj.PExpireAt(key, khash, when)
}

func (b *Bitalos) Del(khash uint32, keys ...[]byte) (int64, error) {
	return b.bitsdb.StringObj.Del(khash, keys...)
}
