// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
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

import (
	"github.com/zuoyebang/bitalostored/stored/engine/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
)

func (b *Bitalos) SetBit64(key []byte, khash uint32, offset int, on int) (int64, error) {
	if offset < 0 {
		return 0, errn.ErrBitOffset
	} else if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	} else if (on & ^1) != 0 {
		return 0, errn.ErrBitValue
	}

	unlockKey := b.bitsdb.LockKey(khash)
	defer unlockKey()

	ret, err := b.bitsdb.DB.SetBit64(key, btools.GetSlotId(khash), uint64(offset), on)
	if err != nil {
		return 0, err
	}

	return ret, nil
}

func (b *Bitalos) GetBit64(key []byte, khash uint32, offset int) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	return b.bitsdb.DB.GetBit64(key, btools.GetSlotId(khash), uint64(offset))
}

func (b *Bitalos) BitCount64(key []byte, khash uint32, start, end int) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	return b.bitsdb.DB.BitCount64(key, btools.GetSlotId(khash), start, end)
}

func (b *Bitalos) BitPos64(key []byte, khash uint32, on, start, end int) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	} else if (on & ^1) != 0 {
		return 0, errn.ErrBitValue
	}

	return b.bitsdb.DB.BitPos64(key, btools.GetSlotId(khash), on, start, end)
}
