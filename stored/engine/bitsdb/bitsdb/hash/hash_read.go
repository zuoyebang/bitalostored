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

package hash

import (
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
)

func (ho *HashObject) HLen(key []byte, khash uint32) (int64, error) {
	return ho.BaseSize(key, khash)
}

func (ho *HashObject) HGet(key []byte, khash uint32, field []byte) ([]byte, func(), error) {
	if err := btools.CheckKeyAndFieldSize(key, field); err != nil {
		return nil, nil, err
	}

	mkv, err := ho.GetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return nil, nil, err
	}
	defer base.PutMkvToPool(mkv)

	ekf, ekfCloser := base.EncodeDataKey(mkv.Version(), khash, field)
	defer ekfCloser()

	value, exist, closer, errno := ho.GetDataValue(ekf)
	if errno != nil || value == nil || !exist {
		return nil, closer, errno
	}

	return value, closer, nil
}

func (ho *HashObject) HMget(key []byte, khash uint32, args ...[]byte) ([][]byte, []func(), error) {
	fieldNum := len(args)
	res := make([][]byte, fieldNum)
	if err := btools.CheckKeySize(key); err != nil {
		return res, nil, err
	}

	mkv, err := ho.GetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return res, nil, err
	}
	defer base.PutMkvToPool(mkv)

	var valClosers []func()
	keyVersion := mkv.Version()
	for i := 0; i < len(args); i++ {
		if e := btools.CheckFieldSize(args[i]); e != nil {
			continue
		}

		ekf, ekfCloser := base.EncodeDataKey(keyVersion, khash, args[i])
		val, _, valCloser, _ := ho.GetDataValue(ekf)
		ekfCloser()
		if val != nil {
			res[i] = val
		}
		if valCloser != nil {
			valClosers = append(valClosers, valCloser)
		}
	}

	return res, valClosers, nil
}

func (ho *HashObject) HGetAll(key []byte, khash uint32) ([]btools.FVPair, []func(), error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, nil, err
	}

	mkv, err := ho.GetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return nil, nil, err
	}
	defer base.PutMkvToPool(mkv)

	var kClosers []func()
	res := make([]btools.FVPair, 0, mkv.Size())
	keyVersion := mkv.Version()

	var lowerBound [base.DataKeyHeaderLength]byte
	var upperBound [base.DataKeyUpperBoundLength]byte
	base.EncodeDataKeyLowerBound(lowerBound[:], keyVersion, khash)
	base.EncodeDataKeyUpperBound(upperBound[:], keyVersion, khash)
	iterOpts := &bitskv.IterOptions{
		KeyHash:    khash,
		UpperBound: upperBound[:],
	}
	it := ho.DataDb.NewIterator(iterOpts)
	defer it.Close()
	for it.Seek(lowerBound[:]); it.Valid(); it.Next() {
		k, kCloser := it.KeyByPools()
		if kCloser != nil {
			kClosers = append(kClosers, kCloser)
		}

		f, version, e := base.DecodeDataKey(k)
		if e != nil || version != keyVersion {
			continue
		}

		v, vCloser := it.ValueByPools()
		if vCloser != nil {
			kClosers = append(kClosers, vCloser)
		}
		res = append(res, btools.FVPair{Field: f, Value: v})
	}

	return res, kClosers, nil
}

func (ho *HashObject) HKeys(key []byte, khash uint32) ([][]byte, []func(), error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, nil, err
	}

	mkv, err := ho.GetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return nil, nil, err
	}
	defer base.PutMkvToPool(mkv)

	var kClosers []func()
	res := make([][]byte, 0, mkv.Size())
	keyVersion := mkv.Version()

	var lowerBound [base.DataKeyHeaderLength]byte
	var upperBound [base.DataKeyUpperBoundLength]byte
	base.EncodeDataKeyLowerBound(lowerBound[:], keyVersion, khash)
	base.EncodeDataKeyUpperBound(upperBound[:], keyVersion, khash)
	iterOpts := &bitskv.IterOptions{
		KeyHash:    khash,
		UpperBound: upperBound[:],
	}
	it := ho.DataDb.NewIterator(iterOpts)
	defer it.Close()
	for it.Seek(lowerBound[:]); it.Valid(); it.Next() {
		k, kCloser := it.KeyByPools()
		if kCloser != nil {
			kClosers = append(kClosers, kCloser)
		}

		f, version, e := base.DecodeDataKey(k)
		if e != nil || version != keyVersion {
			continue
		}
		res = append(res, f)
	}

	return res, kClosers, nil
}

func (ho *HashObject) HValues(key []byte, khash uint32) ([][]byte, []func(), error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, nil, err
	}

	mkv, err := ho.GetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return nil, nil, err
	}
	defer base.PutMkvToPool(mkv)

	var vClosers []func()
	var lowerBound [base.DataKeyHeaderLength]byte
	var upperBound [base.DataKeyUpperBoundLength]byte
	res := make([][]byte, 0, mkv.Size())
	keyVersion := mkv.Version()
	base.EncodeDataKeyLowerBound(lowerBound[:], keyVersion, khash)
	base.EncodeDataKeyUpperBound(upperBound[:], keyVersion, khash)
	iterOpts := &bitskv.IterOptions{
		KeyHash:    khash,
		UpperBound: upperBound[:],
	}
	it := ho.DataDb.NewIterator(iterOpts)
	defer it.Close()
	for it.Seek(lowerBound[:]); it.Valid(); it.Next() {
		_, version, e := base.DecodeDataKey(it.RawKey())
		if e != nil || version != keyVersion {
			continue
		}

		v, vCloser := it.ValueByPools()
		if vCloser != nil {
			vClosers = append(vClosers, vCloser)
		}
		res = append(res, v)
	}

	return res, vClosers, nil
}

func (ho *HashObject) HScan(
	key []byte, khash uint32, cursor []byte, count int, match string,
) ([]byte, []btools.FVPair, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, nil, err
	}

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := ho.GetMetaData(mk)
	if err != nil {
		return nil, nil, err
	}
	defer base.PutMkvToPool(mkv)
	if !mkv.IsAlive() {
		return btools.ScanEndCurosr, nil, nil
	}

	count = btools.CheckScanCount(count)
	getCount := count + 1
	r, err := btools.BuildMatchRegexp(match)
	if err != nil {
		return nil, nil, err
	}

	keyVersion := mkv.Version()
	v := make([]btools.FVPair, 0, getCount)
	seekKey, seekKeyCloser := base.EncodeDataKey(keyVersion, khash, cursor)
	it := ho.DataDb.NewIterator(&bitskv.IterOptions{KeyHash: khash})
	defer func() {
		it.Close()
		seekKeyCloser()
	}()
	it.Seek(seekKey)
	for i := 0; it.Valid() && i < getCount; it.Next() {
		itKeyField, itKeyVersion, err := base.DecodeDataKey(it.Key())
		if err != nil {
			continue
		} else if keyVersion != itKeyVersion {
			break
		} else if len(match) > 0 && !r.Match(unsafe2.String(itKeyField)) {
			continue
		}

		v = append(v, btools.FVPair{
			Field: itKeyField,
			Value: it.Value(),
		})
		i++
		if i >= getCount {
			break
		}
	}

	if len(v) == getCount {
		cursor = v[count].Field
		v = v[:count]
	} else {
		cursor = btools.ScanEndCurosr
	}

	return cursor, v, nil
}
