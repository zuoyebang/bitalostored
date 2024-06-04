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

package btools

import (
	"strconv"

	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/glob"
)

func StrInt64(v []byte, err error) (int64, error) {
	if err != nil {
		return 0, err
	} else if v == nil {
		return 0, nil
	} else {
		return strconv.ParseInt(string(v), 10, 64)
	}
}

func StrFloat64(v []byte, err error) (float64, error) {
	if err != nil {
		return 0, err
	} else if v == nil {
		return 0, nil
	} else {
		if r, e := strconv.ParseFloat(string(v), 64); e != nil {
			return 0, err
		} else {
			return r, err
		}
	}
}

func BuildMatchRegexp(match string) (glob.Glob, error) {
	var err error
	var r glob.Glob

	if len(match) > 0 {
		if r, err = glob.Compile(match); err != nil {
			return nil, err
		}
	}

	return r, nil
}

func CheckScanCount(count int) int {
	if count <= 0 {
		count = DefaultScanCount
	}

	return count
}

func CheckKeySize(key []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return errn.ErrKeySize
	}
	return nil
}

func CheckValueSize(value []byte) error {
	if len(value) > MaxValueSize {
		return errn.ErrValueSize
	}
	return nil
}

func CheckFieldSize(field []byte) error {
	if len(field) > MaxFieldSize || len(field) == 0 {
		return errn.ErrFieldSize
	}
	return nil
}

func CheckKeyAndFieldSize(key []byte, field []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return errn.ErrKeySize
	} else if len(field) > MaxFieldSize || len(field) == 0 {
		return errn.ErrFieldSize
	}
	return nil
}
