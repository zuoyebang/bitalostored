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

package zset

import (
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/dbconfig"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
)

type ZSetObject struct {
	base.BaseObject
}

func NewZSetObject(baseDb *base.BaseDB, cfg *dbconfig.Config) *ZSetObject {
	zo := &ZSetObject{
		BaseObject: base.NewBaseObject(baseDb, cfg, btools.ZSET),
	}
	return zo
}

func (zo *ZSetObject) Close() {
	zo.BaseObject.Close()
}

func (zo *ZSetObject) setZsetIndexValue(
	wb *bitskv.WriteBatch, version uint64, kind uint8, khash uint32, score float64, member []byte,
) error {
	ek, ekCloser, isCompress := base.EncodeZsetIndexKey(version, kind, khash, score, member)
	err := base.SetDataValue(wb, ek, member, isCompress)
	ekCloser()
	return err
}

func (zo *ZSetObject) deleteZsetIndexKey(
	wb *bitskv.WriteBatch, version uint64, kind uint8, khash uint32, score float64, member []byte,
) (err error) {
	mk, mkCloser, _ := base.EncodeZsetIndexKey(version, kind, khash, score, member)
	err = wb.Delete(mk)
	mkCloser()
	return err
}

func (zo *ZSetObject) getZsetValue(key []byte, khash uint32, field []byte) ([]byte, bool, func(), error) {
	mkv, err := zo.GetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return nil, false, nil, err
	}
	defer base.PutMkvToPool(mkv)

	var ekf [base.DataKeyZsetLength]byte
	base.EncodeZsetDataKey(ekf[:], mkv.Version(), khash, field)

	return zo.GetDataValue(ekf[:])
}

func (zo *ZSetObject) zrank(key []byte, khash uint32, member []byte, reverse bool) (int64, error) {
	if err := btools.CheckKeyAndFieldSize(key, member); err != nil {
		return 0, err
	}

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := zo.GetMetaData(mk)
	if err != nil {
		return 0, err
	}
	defer base.PutMkvToPool(mkv)
	if !mkv.IsAlive() {
		return 0, errn.ErrZsetMemberNil
	}

	var ekf [base.DataKeyZsetLength]byte
	keyVersion := mkv.Version()
	keyKind := mkv.Kind()
	base.EncodeZsetDataKey(ekf[:], keyVersion, khash, member)
	_, fexist, fCloser, err := zo.GetDataValue(ekf[:])
	defer func() {
		if fCloser != nil {
			fCloser()
		}
	}()
	if err != nil {
		return 0, err
	}
	if !fexist {
		return 0, errn.ErrZsetMemberNil
	}

	var find bool
	var index int64
	var lowerBound [base.DataKeyHeaderLength]byte
	var upperBound [base.IndexKeyScoreLength]byte
	base.EncodeDataKeyLowerBound(lowerBound[:], keyVersion, khash)
	base.EncodeZsetIndexKeyUpperBound(upperBound[:], keyVersion, khash)
	iterOpts := &bitskv.IterOptions{
		KeyHash:    khash,
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
	}
	it := zo.DataDb.NewIteratorIndex(iterOpts)
	defer it.Close()
	if !reverse {
		totalIndex := mkv.Size() - 1
		for it.Seek(lowerBound[:]); it.Valid(); it.Next() {
			version, _, m := base.DecodeZsetIndexKey(keyKind, it.RawKey(), it.RawValue())
			if keyVersion != version {
				break
			}
			if m.Equal(member) {
				find = true
				break
			}
			index++
			if index > totalIndex {
				break
			}
		}
	} else {
		left := mkv.Size()
		for it.SeekLT(upperBound[:]); it.Valid(); it.Prev() {
			version, _, m := base.DecodeZsetIndexKey(keyKind, it.RawKey(), it.RawValue())
			if keyVersion != version {
				break
			}
			if m.Equal(member) {
				find = true
				break
			}
			index++
			left--
			if left < 0 {
				break
			}
		}
	}

	if find {
		return index, nil
	}
	return 0, errn.ErrZsetMemberNil
}

func (zo *ZSetObject) zParseLimit(size int64, start int64, stop int64, reverse bool) (startIndex int64, stopIndex int64) {
	if !reverse {
		if start >= 0 {
			startIndex = start
		} else {
			startIndex = size + start
		}
		if stop >= 0 {
			stopIndex = stop
		} else {
			stopIndex = size + stop
		}
	} else {
		if stop >= 0 {
			startIndex = size - stop - 1
		} else {
			startIndex = -stop - 1
		}
		if start >= 0 {
			stopIndex = size - start - 1
		} else {
			stopIndex = -start - 1
		}
	}

	if startIndex <= 0 {
		startIndex = 0
	}
	if stopIndex >= size {
		stopIndex = size - 1
	}
	return
}
