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

package rstring

import (
	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"
)

func (so *StringObject) TTL(key []byte, khash uint32) (int64, error) {
	return so.BasePTTL(key, khash, false)
}

func (so *StringObject) PTTL(key []byte, khash uint32) (int64, error) {
	return so.BasePTTL(key, khash, true)
}

func (so *StringObject) Type(key []byte, khash uint32) (string, error) {
	return so.BaseType(key, khash)
}

func (so *StringObject) Exists(key []byte, khash uint32) (int64, error) {
	return so.BaseExists(key, khash)
}

func (so *StringObject) Get(key []byte, khash uint32) ([]byte, func(), error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, nil, err
	}

	ek, ekCloser := base.EncodeMetaKey(key, khash)
	defer ekCloser()

	value, _, closer, err := so.getValueCheckAliveForString(ek)
	return value, closer, err
}

func (so *StringObject) GetWithTTL(key []byte, khash uint32) ([]byte, func(), int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, nil, base.ErrnoKeyNotFoundOrExpire, err
	}

	var ttl int64
	ek, ekCloser := base.EncodeMetaKey(key, khash)
	defer ekCloser()

	value, timestamp, closer, err := so.getValueForString(ek)
	if value == nil {
		ttl = base.ErrnoKeyNotFoundOrExpire
	} else if timestamp == 0 {
		ttl = base.ErrnoKeyPersist
	} else {
		ttl = int64(timestamp) - tclock.GetTimestampMilli()
		if ttl <= 0 {
			ttl = base.ErrnoKeyNotFoundOrExpire
		}
	}

	return value, closer, ttl, err
}

func (so *StringObject) MGet(khash uint32, keys ...[]byte) ([][]byte, []func(), error) {
	keyNum := len(keys)
	eks := make([][]byte, keyNum)
	ekClosers := make([]func(), keyNum)
	vals := make([][]byte, keyNum)
	valClosers := make([]func(), keyNum)

	var isHashTag bool
	firstKeyHash := hash.Fnv32(keys[0])
	if firstKeyHash != khash {
		isHashTag = true
		firstKeyHash = khash
	}

	for i, key := range keys {
		if i == 0 {
			khash = firstKeyHash
		} else if !isHashTag {
			khash = hash.Fnv32(key)
		}
		if err := btools.CheckKeySize(keys[i]); err == nil {
			eks[i], ekClosers[i] = base.EncodeMetaKey(key, khash)
		}
	}

	defer func() {
		for _, ekCloser := range ekClosers {
			if ekCloser != nil {
				ekCloser()
			}
		}
	}()

	for i, ek := range eks {
		if ek != nil {
			vals[i], _, valClosers[i], _ = so.getValueCheckAliveForString(ek)
		}
	}

	return vals, valClosers, nil
}

func (so *StringObject) GetRange(key []byte, khash uint32, start int, end int) ([]byte, func(), error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, nil, err
	}

	ek, ekCloser := base.EncodeMetaKey(key, khash)
	defer ekCloser()

	value, _, valCloser, err := so.getValueCheckAliveForString(ek)
	if err != nil || value == nil {
		return []byte{}, valCloser, err
	}

	valLen := len(value)
	begin, stop, hasRange := getSliceRange(start, end, valLen)
	if !hasRange {
		return []byte{}, nil, nil
	}
	return value[begin : stop+1], valCloser, nil
}

func (so *StringObject) StrLen(key []byte, khash uint32) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	ek, ekCloser := base.EncodeMetaKey(key, khash)
	value, _, valueCloser, err := so.getValueCheckAliveForString(ek)
	defer func() {
		ekCloser()
		if valueCloser != nil {
			valueCloser()
		}
	}()
	if err != nil {
		return 0, err
	}
	return int64(len(value)), nil
}
