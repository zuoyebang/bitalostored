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

package bitskv

import (
	"bytes"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv/kv"
	"github.com/zuoyebang/bitalostored/stored/internal/bytepools"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

type Iterator struct {
	it    kv.IIterator
	key   []byte
	value []byte
}

func (it *Iterator) ValidForPrefix(prefix []byte) bool {
	return bytes.HasPrefix(it.RawKey(), prefix)
}

func (it *Iterator) Key() []byte {
	it.key = it.it.Key()

	if it.key == nil {
		return nil
	}

	return append([]byte{}, it.key...)
}

func (it *Iterator) Value() []byte {
	it.value = it.it.Value()
	if it.value == nil {
		return nil
	}

	return append([]byte{}, it.value...)
}

func (it *Iterator) RawKey() []byte {
	it.key = it.it.Key()
	return it.key
}

func (it *Iterator) RawValue() []byte {
	it.value = it.it.Value()
	return it.value
}

func (it *Iterator) KeyByPools() ([]byte, func()) {
	it.key = it.it.Key()
	if it.key == nil {
		return nil, nil
	}

	return bytepools.BytePools.MakeValue(it.key)
}

func (it *Iterator) ValueByPools() ([]byte, func()) {
	it.value = it.it.Value()
	if it.value == nil {
		return nil, nil
	}

	return bytepools.BytePools.MakeValue(it.value)
}

func (it *Iterator) Close() {
	if it.it != nil {
		if err := it.it.Close(); err != nil {
			log.Errorf("Iterator close err [err:%s]", err.Error())
		}
		it.it = nil
	}
}

func (it *Iterator) Valid() bool {
	return it.it.Valid()
}

func (it *Iterator) Next() {
	it.it.Next()
}

func (it *Iterator) Prev() {
	it.it.Prev()
}

func (it *Iterator) First() {
	it.it.First()
}

func (it *Iterator) Last() {
	it.it.Last()
}

func (it *Iterator) Seek(key []byte) {
	it.it.SeekGE(key)
}

func (it *Iterator) SeekLT(key []byte) {
	it.it.SeekLT(key)
}
