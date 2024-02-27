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

func (b *Bitalos) SAdd(key []byte, khash uint32, args ...[]byte) (int64, error) {
	return b.bitsdb.SetObj.SAdd(key, khash, args...)
}

func (b *Bitalos) SCard(key []byte, khash uint32) (int64, error) {
	return b.bitsdb.SetObj.SCard(key, khash)
}

func (b *Bitalos) SClear(khash uint32, key ...[]byte) (int64, error) {
	return b.bitsdb.SetObj.Del(khash, key...)
}

func (b *Bitalos) SIsMember(key []byte, khash uint32, member []byte) (int64, error) {
	return b.bitsdb.SetObj.SIsMember(key, khash, member)
}

func (b *Bitalos) SMembers(key []byte, khash uint32) ([][]byte, error) {
	return b.bitsdb.SetObj.SMembers(key, khash)
}

func (b *Bitalos) SRandMember(key []byte, khash uint32, count int64) ([][]byte, error) {
	return b.bitsdb.SetObj.SRandMember(key, khash, count)
}

func (b *Bitalos) SPop(key []byte, khash uint32, count int64) ([][]byte, error) {
	return b.bitsdb.SetObj.SPop(key, khash, count)
}

func (b *Bitalos) SRem(key []byte, khash uint32, args ...[]byte) (int64, error) {
	return b.bitsdb.SetObj.SRem(key, khash, args...)
}
