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

package list

import (
	"math"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
)

func (lo *ListObject) LIndex(key []byte, khash uint32, index int64) ([]byte, func(), error) {
	if len(key) > btools.MaxKeySize {
		return nil, nil, errn.ErrKeySize
	}
	if index > int64(math.MaxUint32) {
		return nil, nil, errIndexOverflow
	}

	mkv, err := lo.GetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return nil, nil, err
	}
	defer base.PutMkvToPool(mkv)
	if index > mkv.Size() || -index > mkv.Size() {
		return nil, nil, nil
	}

	var seq int64
	if index >= 0 {
		lindex := mkv.GetLeftElementIndex()
		seq = int64(lindex) + index
	} else {
		rindex := mkv.GetRightElementIndex()
		seq = int64(rindex) + index + 1
	}
	readIndexBuf := extend.Uint32ToBytes(uint32(seq))
	ekf, ekfCloser := base.EncodeDataKey(mkv.Version(), khash, readIndexBuf)
	defer ekfCloser()
	value, _, vcloser, e := lo.GetDataValue(ekf)
	return value, vcloser, e
}

func (lo *ListObject) LLen(key []byte, khash uint32) (int64, error) {
	return lo.BaseSize(key, khash)
}

func (lo *ListObject) LRange(key []byte, khash uint32, start int64, stop int64) ([][]byte, error) {
	if len(key) > btools.MaxKeySize {
		return nil, errn.ErrKeySize
	}

	mkv, err := lo.GetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return nil, err
	}
	defer base.PutMkvToPool(mkv)

	var findStart, findStop uint32

	llen := mkv.Size()
	if llen <= 0 {
		return [][]byte{}, nil
	}
	if start < 0 {
		if tmp := llen + start; tmp < 0 {
			findStart = 0
		} else {
			findStart = uint32(tmp)
		}
	} else {
		findStart = uint32(start)
	}

	if stop < 0 {
		if tmp := llen + stop; tmp < 0 {
			return [][]byte{}, nil
		} else {
			findStop = uint32(tmp)
		}
	} else {
		findStop = uint32(stop)
	}

	if findStart >= uint32(llen) || findStart > findStop {
		return [][]byte{}, nil
	}
	if findStop >= uint32(llen) {
		findStop = uint32(llen) - 1
	}
	if findStop-findStart >= ListReadMax-1 {
		findStop = findStart + ListReadMax - 1
	}

	leftStartIndex := mkv.GetLeftElementIndex()
	rightStopIndex := mkv.GetRightElementIndex()
	keyVersion := mkv.Version()
	lindex := leftStartIndex + findStart
	rindex := leftStartIndex + findStop
	var seekEkf [base.DataKeyListIndex]byte
	base.EncodeListDataKeyIndex(seekEkf[:], keyVersion, khash, lindex)
	limit := int(findStop - findStart + 1)

	var it *bitskv.Iterator
	res := make([][]byte, 0, limit)

	iters := lo.getIter(khash, mkv)
	defer func() {
		for i := range iters {
			iters[i].Close()
		}
	}()

	if len(iters) == 1 {
		it = iters[0]
		idx := lindex
		for it.Seek(seekEkf[:]); it.Valid() && idx <= rindex; it.Next() {
			res = append(res, it.Value())
			idx++
			if idx > rindex {
				break
			}
		}
	} else {
		if lindex >= leftStartIndex && rindex > rightStopIndex {
			it = iters[0]
			idx := lindex
			for it.Seek(seekEkf[:]); it.Valid() && idx <= rindex; it.Next() {
				res = append(res, it.Value())
				idx++
				if idx > rindex {
					break
				}
			}
		} else if lindex <= rightStopIndex && rindex <= rightStopIndex {
			it = iters[1]
			idx := uint32(0)
			for it.Seek(seekEkf[:]); it.Valid() && idx <= rindex; it.Next() {
				res = append(res, it.Value())
				idx++
				if idx > rindex {
					break
				}
			}
		} else {
			it = iters[0]
			idx := lindex
			for it.Seek(seekEkf[:]); it.Valid() && idx <= base.MaxIndex; it.Next() {
				res = append(res, it.Value())
				if idx == base.MaxIndex {
					break
				}
				idx++
			}
			it = iters[1]
			idx = 0
			for it.First(); it.Valid() && idx <= rindex; it.Next() {
				res = append(res, it.Value())
				idx++
				if idx > rindex {
					break
				}
			}
		}
	}

	return res, nil
}
