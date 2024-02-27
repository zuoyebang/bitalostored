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
	"fmt"
	"math"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"

	"github.com/RoaringBitmap/roaring/roaring64"
)

func (so *StringObject) SetBit(key []byte, khash uint32, offset int, on int) (int64, error) {
	if offset < 0 {
		return 0, resp.ErrBitOffset
	}
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}
	if (on & ^1) != 0 {
		return 0, resp.ErrBitValue
	}

	unlockKey := so.LockKey(khash)
	defer unlockKey()

	ek, ekCloser := base.EncodeMetaKey(key, khash)
	value, timestamp, valCloser, err := so.getValueCheckAliveForString(ek)
	defer func() {
		ekCloser()
		if valCloser != nil {
			valCloser()
		}
	}()
	if err != nil {
		return 0, err
	}

	x := uint64(offset)
	var (
		isExist bool
		changed bool
		ret     int64
	)

	rb := roaring64.NewBitmap()
	if value != nil {
		if err = rb.UnmarshalBinary(value); err != nil {
			return 0, resp.ErrBitUnmarshal
		}
		isExist = rb.Contains(x)
	}

	if isExist {
		ret = 1
	}

	if on == 1 {
		if isExist {
			return ret, nil
		}

		rb.Add(x)
		changed = true
	} else if on == 0 {
		if isExist {
			rb.Remove(x)
			changed = true
		}
	}

	if changed {
		if rb.GetCardinality() == 0 {
			if err = so.BaseDb.DeleteMetaKey(ek); err != nil {
				return 0, err
			}
		} else {
			value, err = rb.MarshalBinary()
			if err != nil {
				return 0, resp.ErrBitMarshal
			}

			if err = so.setValueForString(ek, value, timestamp); err != nil {
				return 0, err
			}
		}
	}

	return ret, nil
}

func (so *StringObject) GetBit(key []byte, khash uint32, offset int) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	ek, ekCloser := base.EncodeMetaKey(key, khash)
	value, _, valCloser, err := so.getValueCheckAliveForString(ek)
	defer func() {
		ekCloser()
		if valCloser != nil {
			valCloser()
		}
	}()
	if err != nil {
		return 0, err
	}

	if value == nil {
		return 0, nil
	}

	rb := roaring64.NewBitmap()
	if err = rb.UnmarshalBinary(value); err != nil {
		return 0, resp.ErrBitUnmarshal
	}

	if rb.Contains(uint64(offset)) {
		return 1, nil
	} else {
		return 0, nil
	}
}

func (so *StringObject) BitCount(key []byte, khash uint32, start int, end int) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	ek, ekCloser := base.EncodeMetaKey(key, khash)
	value, _, valCloser, err := so.getValueCheckAliveForString(ek)
	defer func() {
		ekCloser()
		if valCloser != nil {
			valCloser()
		}
	}()
	if err != nil {
		return 0, err
	}

	if value == nil {
		return 0, nil
	}

	rb := roaring64.NewBitmap()
	if err = rb.UnmarshalBinary(value); err != nil {
		return 0, resp.ErrBitUnmarshal
	}

	if start == 0 && end == -1 {
		return int64(rb.GetCardinality()), nil
	}

	begin, stop, hasRange := getBitmapRange(start, end)
	if !hasRange {
		return 0, nil
	}

	var (
		n int64
		x uint64
	)

	i := rb.Iterator()
	i.AdvanceIfNeeded(begin)
	for i.HasNext() {
		x = i.Next()
		if x < begin || x > stop {
			break
		}
		n++
	}

	return n, nil
}

func (so *StringObject) BitPos(key []byte, khash uint32, on int, start int, end int) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}
	if (on & ^1) != 0 {
		return 0, fmt.Errorf("bit must be 0 or 1, not %d", on)
	}

	ek, ekCloser := base.EncodeMetaKey(key, khash)
	value, _, valCloser, err := so.getValueCheckAliveForString(ek)
	defer func() {
		ekCloser()
		if valCloser != nil {
			valCloser()
		}
	}()
	if err != nil {
		return -1, err
	}

	if value == nil {
		if on == 1 {
			return -1, nil
		}
		return 0, nil
	}

	rb := roaring64.NewBitmap()
	if err = rb.UnmarshalBinary(value); err != nil {
		return -1, resp.ErrBitUnmarshal
	}

	begin, stop, hasRange := getBitmapRange(start, end)
	if !hasRange {
		return -1, nil
	}

	i := rb.Iterator()
	i.AdvanceIfNeeded(begin)

	var x uint64
	if on == 1 {
		if i.HasNext() {
			x = i.Next()
			if x <= stop {
				return int64(x), nil
			}
		}
		return -1, nil
	} else {
		if !i.HasNext() || i.Next() > begin {
			return int64(begin), nil
		}

		s := begin + 1
		for s <= stop {
			if !rb.Contains(s) {
				break
			}
			s++
		}
		return int64(s), nil
	}
}

func getBitmapRange(start, end int) (uint64, uint64, bool) {
	if start < 0 {
		start = math.MaxInt64 + start + 1
	}

	if end < 0 {
		end = math.MaxInt64 + end + 1
	}

	if start < 0 {
		start = 0
	}

	if end < 0 {
		end = 0
	}

	return uint64(start), uint64(end), start <= end
}
