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

package set

import (
	"math/rand"
	"sort"
	"time"

	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
)

func (so *SetObject) SCard(key []byte, khash uint32) (int64, error) {
	return so.BaseSize(key, khash)
}

func (so *SetObject) SRandMember(key []byte, khash uint32, count int64) ([][]byte, error) {
	if count == 0 {
		return nil, nil
	}

	if err := btools.CheckKeySize(key); err != nil {
		return nil, err
	}

	var randCount int64
	var repeated bool
	if count > 0 {
		randCount = count
		repeated = false
	} else {
		randCount = -count
		repeated = true
	}

	mkv, err := so.GetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return nil, err
	}
	defer base.PutMkvToPool(mkv)

	keyVersion := mkv.Version()
	keyKind := mkv.Kind()
	keySize := mkv.Size()
	randSize := randCount * 2
	if randSize > keySize {
		randSize = keySize
	}
	randNumIndexs := GenRandomNumber(0, int(randSize), int(randCount), repeated)
	if len(randNumIndexs) <= 0 {
		return nil, nil
	}
	sort.Ints(randNumIndexs)

	members := make([][]byte, 0, randCount)

	var cnt int64
	var lowerBound [base.DataKeyHeaderLength]byte
	var upperBound [base.DataKeyUpperBoundLength]byte
	base.EncodeDataKeyLowerBound(lowerBound[:], keyVersion, khash)
	base.EncodeDataKeyUpperBound(upperBound[:], keyVersion, khash)
	iterOpts := &bitskv.IterOptions{
		KeyHash:    khash,
		UpperBound: upperBound[:],
	}
	it := so.DataDb.NewIterator(iterOpts)
	defer it.Close()
	if !repeated {
		for it.Seek(lowerBound[:]); it.Valid(); it.Next() {
			if len(randNumIndexs) <= 0 {
				break
			}
			if int(cnt) == randNumIndexs[0] {
				randNumIndexs = randNumIndexs[1:]
				version, fp := base.DecodeSetDataKey(keyKind, it.RawKey(), it.RawValue())
				if version != keyVersion {
					break
				}
				members = append(members, fp.Merge())
			}
			cnt++
			if cnt >= keySize {
				break
			}
		}
	} else {
		for it.Seek(lowerBound[:]); it.Valid(); {
			if len(randNumIndexs) <= 0 {
				break
			}
			if int(cnt) == randNumIndexs[0] {
				randNumIndexs = randNumIndexs[1:]
				version, fp := base.DecodeSetDataKey(keyKind, it.RawKey(), it.RawValue())
				if version != keyVersion {
					break
				}
				members = append(members, fp.Merge())
			} else if int(cnt) < randNumIndexs[0] {
				cnt++
				if cnt >= keySize {
					break
				}
				it.Next()
			}
		}
	}

	rand.Shuffle(len(members), func(i, j int) {
		members[i], members[j] = members[j], members[i]
	})
	return members, nil
}

func (so *SetObject) SIsMember(key []byte, khash uint32, member []byte) (int64, error) {
	if err := btools.CheckKeyAndFieldSize(key, member); err != nil {
		return 0, err
	}

	mkv, err := so.GetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return 0, err
	}
	defer base.PutMkvToPool(mkv)

	ekf, ekfCloser, _ := base.EncodeSetDataKey(mkv.Version(), mkv.Kind(), khash, member)
	defer ekfCloser()
	var exist bool
	exist, err = so.IsExistData(ekf)
	if err != nil || !exist {
		return 0, err
	}
	return 1, nil
}

func (so *SetObject) SMembers(key []byte, khash uint32) ([][]byte, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, err
	}

	mkv, err := so.GetMetaDataCheckAlive(key, khash)
	if mkv == nil {
		return nil, err
	}
	defer base.PutMkvToPool(mkv)

	var lowerBound [base.DataKeyHeaderLength]byte
	var upperBound [base.DataKeyUpperBoundLength]byte
	res := make([][]byte, 0, mkv.Size())
	keyVersion := mkv.Version()
	keyKind := mkv.Kind()
	base.EncodeDataKeyLowerBound(lowerBound[:], keyVersion, khash)
	base.EncodeDataKeyUpperBound(upperBound[:], keyVersion, khash)
	iterOpts := &bitskv.IterOptions{
		KeyHash:    khash,
		UpperBound: upperBound[:],
	}
	it := so.DataDb.NewIterator(iterOpts)
	defer it.Close()

	for it.Seek(lowerBound[:]); it.Valid(); it.Next() {
		version, fp := base.DecodeSetDataKey(keyKind, it.RawKey(), it.RawValue())
		if version != keyVersion {
			break
		}
		res = append(res, fp.Merge())
	}

	return res, nil
}

func (so *SetObject) SScan(
	key []byte, khash uint32, cursor []byte, count int, match string,
) ([]byte, [][]byte, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, nil, err
	}

	mk, mkCloser := base.EncodeMetaKey(key, khash)
	defer mkCloser()
	mkv, err := so.GetMetaData(mk)
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
	keyKind := mkv.Kind()
	res := make([][]byte, 0, getCount)
	seekKey, seekKeyCloser := base.EncodeDataKeyCursor(keyVersion, khash, cursor)
	it := so.DataDb.NewIterator(&bitskv.IterOptions{KeyHash: khash})
	defer func() {
		it.Close()
		seekKeyCloser()
	}()
	it.Seek(seekKey)
	for i := 0; it.Valid() && i < getCount; it.Next() {
		version, fp, cur := base.DecodeDataKeyCursor(keyKind, it.RawKey(), it.RawValue())
		if keyVersion != version {
			break
		}

		member := fp.Merge()
		if len(match) > 0 && !r.Match(unsafe2.String(member)) {
			continue
		}

		res = append(res, member)
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

func GenRandomNumber(start int, end int, count int, repeated bool) []int {
	if end < start {
		return nil
	}

	var nums []int

	if repeated {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for len(nums) < count {
			nums = append(nums, r.Intn(end-start)+start)
		}
	} else {
		if end-start <= count {
			for i := start; i < end; i++ {
				nums = append(nums, i)
			}
		} else {
			numsMap := make(map[int]bool, count)
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for len(nums) < count {
				num := r.Intn(end-start) + start
				if !numsMap[num] {
					numsMap[num] = true
					nums = append(nums, num)
				}
			}
		}
	}

	return nums
}
