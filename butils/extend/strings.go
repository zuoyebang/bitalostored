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

package extend

import (
	"strconv"
	"unicode"
)

func ParseUint(s string) (uint, error) {
	if v, err := strconv.ParseUint(s, 10, 0); err != nil {
		return 0, err
	} else {
		return uint(v), nil
	}
}

func ParseUint8(s string) (uint8, error) {
	if v, err := strconv.ParseUint(s, 10, 8); err != nil {
		return 0, err
	} else {
		return uint8(v), nil
	}
}

func ParseUint16(s string) (uint16, error) {
	if v, err := strconv.ParseUint(s, 10, 16); err != nil {
		return 0, err
	} else {
		return uint16(v), nil
	}
}

func ParseUint32(s string) (uint32, error) {
	if v, err := strconv.ParseUint(s, 10, 32); err != nil {
		return 0, err
	} else {
		return uint32(v), nil
	}
}

func ParseUint64(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}

func ParseInt(s string) (int, error) {
	if v, err := strconv.ParseInt(s, 10, 0); err != nil {
		return 0, err
	} else {
		return int(v), nil
	}
}

func StringToInt(s string) int {
	i, _ := strconv.Atoi(s)
	return i
}

func ParseInt8(s string) (int8, error) {
	if v, err := strconv.ParseInt(s, 10, 8); err != nil {
		return 0, err
	} else {
		return int8(v), nil
	}
}

func ParseInt16(s string) (int16, error) {
	if v, err := strconv.ParseInt(s, 10, 16); err != nil {
		return 0, err
	} else {
		return int16(v), nil
	}
}

func ParseInt32(s string) (int32, error) {
	if v, err := strconv.ParseInt(s, 10, 32); err != nil {
		return 0, err
	} else {
		return int32(v), nil
	}
}

func ParseInt64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

func ParseFloat64(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

func IsNumeric(s string) bool {
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

// SliceExists checks if a string is in a set.
func SliceExists(set []string, find string) bool {
	for _, s := range set {
		if s == find {
			return true
		}
	}
	return false
}
