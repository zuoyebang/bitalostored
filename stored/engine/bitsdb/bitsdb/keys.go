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

package bitsdb

import (
	"bytes"
	"encoding/binary"

	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/glob"
)

func (bdb *BitsDB) Scan(
	cursor []byte, count int, match string, dt btools.DataType,
) ([]byte, [][]byte, error) {
	var (
		ek  []byte
		r   glob.Glob
		err error
	)

	if len(cursor) == 0 || bytes.Equal(cursor, []byte{'0'}) {
		ek = nil
	} else {
		khash := hash.Fnv32(cursor)
		var ekCloser func()
		ek, ekCloser = base.EncodeMetaKey(cursor, khash)
		defer ekCloser()
	}

	if len(match) > 0 {
		if match == "*" {
			match = ""
		} else {
			r, err = btools.BuildMatchRegexp(match)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	count = btools.CheckScanCount(count)
	getCount := count + 1

	mkv := base.GetMkvFromPool()
	defer base.PutMkvToPool(mkv)
	v := make([][]byte, 0, getCount)

	iterOpts := &bitskv.IterOptions{IsAll: true}
	it := bdb.StringObj.BaseDb.DB.NewIteratorMeta(iterOpts)
	defer it.Close()

	if ek == nil {
		it.First()
	} else {
		it.Seek(ek)
	}
	for i := 0; it.Valid() && i < getCount; it.Next() {
		key, err := base.DecodeMetaKey(it.Key())
		if err != nil {
			return nil, nil, err
		}

		if len(match) > 0 && !r.Match(unsafe2.String(key)) {
			continue
		}

		if err := base.DecodeMetaValue(mkv, it.RawValue()); err != nil {
			return nil, nil, err
		}

		if dt != btools.NoneType && mkv.GetDataType() != dt {
			continue
		}

		if mkv.IsAlive() {
			v = append(v, key)
			i++
		}
	}

	if len(v) == getCount {
		cursor = v[count]
		v = v[:count]
	} else {
		cursor = btools.ScanEndCurosr
	}

	return cursor, v, nil
}

func (bdb *BitsDB) ScanBySlotId(
	slotId uint32, cursor []byte, count int, match string,
) ([]byte, []btools.ScanPair, error) {
	r, err := btools.BuildMatchRegexp(match)
	if err != nil {
		return btools.ScanEndCurosr, nil, err
	}

	var mk []byte
	var slotIdPrefix [2]byte
	binary.LittleEndian.PutUint16(slotIdPrefix[:], uint16(slotId))
	if len(cursor) == 0 || bytes.Equal(cursor, []byte{'0'}) {
		mk = slotIdPrefix[:]
	} else {
		var mkCloser func()
		mk, mkCloser = base.EncodeMetaKey(cursor, hash.Fnv32(cursor))
		defer mkCloser()
	}

	//Take one more and use it as the cursor location for the next time
	count = btools.CheckScanCount(count)
	getCount := count + 1
	v := make([]btools.ScanPair, 0, getCount)

	mkv := base.GetMkvFromPool()
	defer base.PutMkvToPool(mkv)

	iterOpts := &bitskv.IterOptions{SlotId: slotId}
	it := bdb.StringObj.BaseDb.DB.NewIteratorMeta(iterOpts)
	defer it.Close()
	i := 0
	for it.Seek(mk); it.Valid() && it.ValidForPrefix(slotIdPrefix[:]); it.Next() {
		key, err := base.DecodeMetaKey(it.Key())
		if err != nil {
			return btools.ScanEndCurosr, nil, err
		}

		mkv.Reset(0)
		if err := base.DecodeMetaValue(mkv, it.RawValue()); err != nil {
			return btools.ScanEndCurosr, nil, err
		}

		if len(match) <= 0 || !r.Match(string(key)) {
			continue
		}

		if mkv.IsAlive() {
			v = append(v, btools.ScanPair{
				Key: key,
				Dt:  mkv.GetDataType(),
			})
			i++
			if i >= getCount {
				break
			}
		}
	}

	if len(v) == getCount {
		cursor = v[count].Key
		v = v[:count]
	} else {
		cursor = btools.ScanEndCurosr
	}

	return cursor, v, nil
}
