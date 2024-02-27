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
	"bytes"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"

	"github.com/zuoyebang/bitalostored/butils/numeric"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
)

func (zo *ZSetObject) ZCard(key []byte, khash uint32) (int64, error) {
	return zo.BaseSize(key, khash)
}

func (zo *ZSetObject) ZScore(key []byte, khash uint32, member []byte) (float64, error) {
	if err := btools.CheckKeyAndFieldSize(key, member); err != nil {
		return 0, err
	}

	value, exist, closer, err := zo.getZsetValue(key, khash, member)
	defer func() {
		if closer != nil {
			closer()
		}
	}()
	if err != nil {
		return 0, err
	}

	if !exist || len(value) != base.ScoreLength {
		return 0, errn.ErrZsetMemberNil
	}

	return numeric.ByteSortToFloat64(value), nil
}

func (zo *ZSetObject) ZCount(
	key []byte, khash uint32, min float64, max float64, leftClose bool, rightClose bool,
) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	mkv, err := zo.GetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return 0, err
	}
	defer base.PutMkvToPool(mkv)
	stopIndex := mkv.Size() - 1
	keyVersion := mkv.Version()
	keyKind := mkv.Kind()
	var n, index int64
	var lowerBound [base.IndexKeyScoreLength]byte
	var upperBound [base.IndexKeyScoreUpperBoundLength]byte
	base.EncodeZsetIndexKeyScore(lowerBound[:], keyVersion, khash, min)
	base.EncodeZsetIndexKeyScoreUpperBound(upperBound[:], keyVersion, khash, max)
	iterOpts := &bitskv.IterOptions{
		KeyHash:    khash,
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
	}
	it := zo.DataDb.NewIteratorIndex(iterOpts)
	defer it.Close()
	for it.Seek(lowerBound[:]); it.Valid(); it.Next() {
		version, score, _ := base.DecodeZsetIndexKey(keyKind, it.RawKey(), nil)
		if keyVersion != version {
			break
		}
		if rightClose && score == max {
			break
		}
		if !leftClose || score > min {
			n++
		}
		index++
		if index > stopIndex {
			break
		}
	}
	return n, nil
}

func (zo *ZSetObject) ZRange(
	key []byte, khash uint32, start int64, stop int64,
) ([]btools.ScorePair, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, err
	}

	mkv, err := zo.GetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return nil, err
	}
	defer base.PutMkvToPool(mkv)

	size := mkv.Size()
	startIndex, stopIndex := zo.zParseLimit(size, start, stop, false)
	if startIndex > stopIndex || startIndex >= size || stopIndex < 0 {
		return nil, nil
	}
	nv := stopIndex - startIndex
	if nv > 256 {
		nv = 256
	}
	res := make([]btools.ScorePair, 0, nv)

	var curIndex int64
	var lowerBound [base.DataKeyHeaderLength]byte
	var upperBound [base.IndexKeyScoreLength]byte
	keyVersion := mkv.Version()
	keyKind := mkv.Kind()
	base.EncodeDataKeyLowerBound(lowerBound[:], keyVersion, khash)
	base.EncodeZsetIndexKeyUpperBound(upperBound[:], keyVersion, khash)
	iterOpts := &bitskv.IterOptions{
		KeyHash:    khash,
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
	}
	it := zo.DataDb.NewIteratorIndex(iterOpts)
	defer it.Close()
	for it.Seek(lowerBound[:]); it.Valid(); it.Next() {
		if curIndex >= startIndex {
			version, score, fp := base.DecodeZsetIndexKey(keyKind, it.RawKey(), it.RawValue())
			if keyVersion != version {
				break
			}
			res = append(res, btools.ScorePair{
				Member: fp.Merge(),
				Score:  score,
			})
		}
		curIndex++
		if curIndex > stopIndex {
			break
		}
	}
	return res, nil
}

func (zo *ZSetObject) ZRevRange(
	key []byte, khash uint32, start int64, stop int64,
) ([]btools.ScorePair, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, err
	}

	mkv, err := zo.GetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return nil, err
	}
	defer base.PutMkvToPool(mkv)

	size := mkv.Size()
	startIndex, stopIndex := zo.zParseLimit(size, start, stop, true)
	if startIndex > stopIndex || startIndex >= size || stopIndex < 0 {
		return nil, nil
	}
	nv := stopIndex - startIndex
	if nv > 256 {
		nv = 256
	}
	res := make([]btools.ScorePair, 0, nv)

	curIndex := size - 1
	keyVersion := mkv.Version()
	keyKind := mkv.Kind()
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
	for it.SeekLT(upperBound[:]); it.Valid(); it.Prev() {
		if curIndex <= stopIndex {
			version, score, fp := base.DecodeZsetIndexKey(keyKind, it.RawKey(), it.RawValue())
			if keyVersion != version {
				break
			}
			res = append(res, btools.ScorePair{
				Member: fp.Merge(),
				Score:  score,
			})
		}
		curIndex--
		if curIndex < startIndex {
			break
		}
	}
	return res, nil
}

func (zo *ZSetObject) ZRangeByScore(
	key []byte, khash uint32, min float64, max float64, leftClose bool, rightClose bool, offset int, count int,
) (res []btools.ScorePair, err error) {
	if offset < 0 {
		return res, nil
	}
	if err := btools.CheckKeySize(key); err != nil {
		return nil, err
	}

	mkv, err := zo.GetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return nil, err
	}
	defer base.PutMkvToPool(mkv)

	stopIndex := mkv.Size() - 1
	skipped := 0
	nv := count

	if nv <= 0 || nv > 256 {
		nv = 256
	}
	res = make([]btools.ScorePair, 0, nv)
	keyVersion := mkv.Version()
	keyKind := mkv.Kind()

	var index int64
	var lowerBound [base.IndexKeyScoreLength]byte
	var upperBound [base.IndexKeyScoreUpperBoundLength]byte
	base.EncodeZsetIndexKeyScore(lowerBound[:], keyVersion, khash, min)
	base.EncodeZsetIndexKeyScoreUpperBound(upperBound[:], keyVersion, khash, max)
	iterOpts := &bitskv.IterOptions{
		KeyHash:    khash,
		UpperBound: upperBound[:],
	}
	it := zo.DataDb.NewIteratorIndex(iterOpts)
	defer it.Close()
	for it.Seek(lowerBound[:]); it.Valid() && index <= stopIndex; it.Next() {
		version, score, fp := base.DecodeZsetIndexKey(keyKind, it.RawKey(), it.RawValue())
		if keyVersion != version {
			break
		}
		if rightClose && score == max {
			break
		}
		if !leftClose || score > min {
			if skipped >= offset {
				res = append(res, btools.ScorePair{
					Member: fp.Merge(),
					Score:  score,
				})
				if count > 0 && len(res) == count {
					break
				}
			}
			skipped++
		}

		index++
		if index > stopIndex {
			break
		}
	}
	return res, nil
}

func (zo *ZSetObject) ZRevRangeByScore(
	key []byte, khash uint32, min float64, max float64, leftClose bool, rightClose bool, offset int, count int,
) ([]btools.ScorePair, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, err
	}

	mkv, err := zo.GetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return nil, err
	}
	defer base.PutMkvToPool(mkv)

	skipped := 0
	nv := count
	if nv <= 0 || nv > 256 {
		nv = 256
	}
	res := make([]btools.ScorePair, 0, nv)
	left := mkv.Size()
	keyVersion := mkv.Version()
	keyKind := mkv.Kind()

	var lowerBound [base.IndexKeyScoreLength]byte
	var upperBound [base.IndexKeyScoreUpperBoundLength]byte
	base.EncodeZsetIndexKeyScore(lowerBound[:], keyVersion, khash, min)
	base.EncodeZsetIndexKeyScoreUpperBound(upperBound[:], keyVersion, khash, max)
	iterOpts := &bitskv.IterOptions{
		KeyHash:    khash,
		LowerBound: lowerBound[:],
		UpperBound: upperBound[:],
	}
	it := zo.DataDb.NewIteratorIndex(iterOpts)
	defer it.Close()
	for it.SeekLT(upperBound[:]); it.Valid() && left > 0; it.Prev() {
		left--
		leftPass := false
		rightPass := false
		version, score, fp := base.DecodeZsetIndexKey(keyKind, it.RawKey(), it.RawValue())
		if keyVersion != version {
			break
		}
		if (leftClose && min < score) || (!leftClose && min <= score) {
			leftPass = true
		}
		if (rightClose && score < max) || (!rightClose && score <= max) {
			rightPass = true
		}
		if leftPass && rightPass {
			if skipped < offset {
				skipped++
				continue
			}
			res = append(res, btools.ScorePair{
				Member: fp.Merge(),
				Score:  score,
			})
			if count > 0 && len(res) == count {
				break
			}
		}
		if !leftPass || left <= 0 {
			break
		}
	}
	return res, nil
}

func (zo *ZSetObject) ZRank(key []byte, khash uint32, member []byte) (int64, error) {
	return zo.zrank(key, khash, member, false)
}

func (zo *ZSetObject) ZRevRank(key []byte, khash uint32, member []byte) (int64, error) {
	return zo.zrank(key, khash, member, true)
}

func (zo *ZSetObject) ZRangeByLex(
	key []byte, khash uint32, min []byte, max []byte, leftClose bool, rightClose bool, offset int, count int,
) ([][]byte, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, err
	}

	mkv, err := zo.GetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return nil, err
	}
	defer base.PutMkvToPool(mkv)

	var leftNoLimit, rightNotLimit bool
	if bytes.Equal([]byte{'-'}, min) {
		leftNoLimit = true
	}
	if bytes.Equal([]byte{'+'}, max) {
		rightNotLimit = true
	}

	res := make([][]byte, 0, 4)
	stopIndex := mkv.Size() - 1
	skipped := 0
	keyVersion := mkv.Version()
	keyKind := mkv.Kind()

	var index int64
	var lowerBound [base.DataKeyHeaderLength]byte
	var upperBound [base.IndexKeyScoreLength]byte
	base.EncodeDataKeyLowerBound(lowerBound[:], keyVersion, khash)
	base.EncodeZsetIndexKeyUpperBound(upperBound[:], keyVersion, khash)
	iterOpts := &bitskv.IterOptions{
		KeyHash:    khash,
		UpperBound: upperBound[:],
	}
	it := zo.DataDb.NewIteratorIndex(iterOpts)
	defer it.Close()
	for it.Seek(lowerBound[:]); it.Valid() && index <= stopIndex; it.Next() {
		leftPass := false
		rightPass := false
		version, _, fp := base.DecodeZsetIndexKey(keyKind, it.RawKey(), it.RawValue())
		if keyVersion != version {
			break
		}
		member := fp.Merge()
		if leftNoLimit ||
			(leftClose && bytes.Compare(min, member) < 0) ||
			(!leftClose && bytes.Compare(min, member) <= 0) {
			leftPass = true
		}
		if rightNotLimit ||
			(rightClose && bytes.Compare(max, member) > 0) ||
			(!rightClose && bytes.Compare(max, member) >= 0) {
			rightPass = true
		}
		if leftPass && rightPass {
			if skipped < offset {
				skipped++
				continue
			}
			res = append(res, member)
			if count > 0 && len(res) == count {
				break
			}
		}
		if !rightPass {
			break
		}
		index++
		if index > stopIndex {
			break
		}
	}

	return res, nil
}

func (zo *ZSetObject) ZLexCount(
	key []byte, khash uint32, min []byte, max []byte, leftClose bool, rightClose bool,
) (int64, error) {
	res, err := zo.ZRangeByLex(key, khash, min, max, leftClose, rightClose, 0, -1)
	if err != nil {
		return 0, err
	}
	return int64(len(res)), nil
}

func (zo *ZSetObject) ZScan(
	key []byte, khash uint32, cursor []byte, count int, match string,
) ([]byte, []btools.ScorePair, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, nil, err
	}

	r, err := btools.BuildMatchRegexp(match)
	if err != nil {
		return nil, nil, err
	}

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := zo.GetMetaData(mk)
	if err != nil {
		return nil, nil, err
	}
	defer base.PutMkvToPool(mkv)
	if !mkv.IsAlive() {
		return btools.ScanEndCurosr, nil, nil
	}

	var res []btools.ScorePair
	var upperBound [base.IndexKeyScoreLength]byte
	count = btools.CheckScanCount(count)
	getCount := count + 1
	keyVersion := mkv.Version()
	keyKind := mkv.Kind()
	seekKey, seekKeyCloser := base.EncodeZsetIndexKeyByCursor(keyVersion, khash, cursor)
	base.EncodeZsetIndexKeyUpperBound(upperBound[:], keyVersion, khash)
	iterOpts := &bitskv.IterOptions{
		KeyHash:    khash,
		UpperBound: upperBound[:],
	}
	it := zo.DataDb.NewIteratorIndex(iterOpts)
	defer func() {
		it.Close()
		seekKeyCloser()
	}()
	i := 0
	for it.Seek(seekKey); it.Valid(); it.Next() {
		version, score, fp, cur := base.DecodeZsetIndexKeyByCursor(keyKind, it.RawKey(), it.RawValue())
		if keyVersion != version {
			break
		}

		member := fp.Merge()
		if len(match) > 0 && !r.Match(unsafe2.String(member)) {
			continue
		}

		res = append(res, btools.ScorePair{
			Score:  score,
			Member: member,
		})
		cursor = cur
		i++
		if i >= getCount {
			break
		}
	}

	if len(res) == getCount {
		res = res[:count]
	} else {
		cursor = btools.ScanEndCurosr
	}

	return cursor, res, nil
}
