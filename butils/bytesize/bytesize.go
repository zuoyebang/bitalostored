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

package bytesize

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
)

const (
	_ = 1 << (10 * iota)
	KB
	MB
	GB
	TB
	PB
)

type Int64 int64

func (b Int64) Int64() int64 {
	return int64(b)
}

func (b Int64) AsInt() int {
	return int(b)
}

func (b Int64) MarshalText() ([]byte, error) {
	if b == 0 {
		return []byte("0"), nil
	}
	var abs = int64(b)
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs%PB == 0:
		val := b.Int64() / PB
		return []byte(fmt.Sprintf("%dpb", val)), nil
	case abs%TB == 0:
		val := b.Int64() / TB
		return []byte(fmt.Sprintf("%dtb", val)), nil
	case abs%GB == 0:
		val := b.Int64() / GB
		return []byte(fmt.Sprintf("%dgb", val)), nil
	case abs%MB == 0:
		val := b.Int64() / MB
		return []byte(fmt.Sprintf("%dmb", val)), nil
	case abs%KB == 0:
		val := b.Int64() / KB
		return []byte(fmt.Sprintf("%dkb", val)), nil
	default:
		return []byte(fmt.Sprintf("%d", b.Int64())), nil
	}
}

func (p *Int64) UnmarshalText(text []byte) error {
	n, err := Parse(string(text))
	if err != nil {
		return err
	}
	*p = Int64(n)
	return nil
}

var (
	fullRegexp = regexp.MustCompile(`^\s*(\-?[\d\.]+)\s*([kmgtp]?b|[bkmgtp]|)\s*$`)
	digitsOnly = regexp.MustCompile(`^\-?\d+$`)
)

var (
	ErrBadByteSize     = errors.New("invalid bytesize")
	ErrBadByteSizeUnit = errors.New("invalid bytesize unit")
)

func Parse(s string) (int64, error) {
	if !fullRegexp.MatchString(s) {
		return 0, ErrBadByteSize
	}

	subs := fullRegexp.FindStringSubmatch(s)
	if len(subs) != 3 {
		return 0, ErrBadByteSize
	}

	text := subs[1]
	unit := subs[2]

	size := int64(1)
	switch unit {
	case "b", "":
	case "k", "kb":
		size = KB
	case "m", "mb":
		size = MB
	case "g", "gb":
		size = GB
	case "t", "tb":
		size = TB
	case "p", "pb":
		size = PB
	default:
		return 0, ErrBadByteSizeUnit
	}

	if digitsOnly.MatchString(text) {
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return 0, ErrBadByteSize
		}
		size *= n
	} else {
		n, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return 0, ErrBadByteSize
		}
		size = int64(float64(size) * n)
	}
	return size, nil
}

func MustParse(s string) int64 {
	v, err := Parse(s)
	if err != nil {
		panic("parse bytesize failed, err : " + err.Error())
	}
	return v
}
