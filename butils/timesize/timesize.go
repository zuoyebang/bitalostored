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

package timesize

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

type Duration time.Duration

func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

func (d Duration) Int64() int64 {
	return int64(d)
}

func (d Duration) MarshalText() ([]byte, error) {
	if d == 0 {
		return []byte("0"), nil
	}
	var abs = time.Duration(d)
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs%time.Hour == 0:
		val := d.Int64() / int64(time.Hour)
		return []byte(fmt.Sprintf("%dh", val)), nil
	case abs%time.Minute == 0:
		val := d.Int64() / int64(time.Minute)
		return []byte(fmt.Sprintf("%dm", val)), nil
	case abs%time.Second == 0:
		val := d.Int64() / int64(time.Second)
		return []byte(fmt.Sprintf("%ds", val)), nil
	case abs%time.Millisecond == 0:
		val := d.Int64() / int64(time.Millisecond)
		return []byte(fmt.Sprintf("%dms", val)), nil
	case abs%time.Microsecond == 0:
		val := d.Int64() / int64(time.Microsecond)
		return []byte(fmt.Sprintf("%dus", val)), nil
	default:
		return []byte(d.Duration().String()), nil
	}
}

func (p *Duration) Set(t time.Duration) {
	*p = Duration(t)
}

func (p *Duration) UnmarshalText(text []byte) error {
	n, err := Parse(string(text))
	if err != nil {
		return err
	}
	*p = Duration(n)
	return nil
}

var (
	fullRegexp = regexp.MustCompile(`^\s*(\-?[\d\.]+)\s*([a-z]+|)\s*$`)
	digitsOnly = regexp.MustCompile(`^\-?\d+$`)
)

var ErrBadTimeSize = errors.New("invalid timesize")

func Parse(s string) (time.Duration, error) {
	if !fullRegexp.MatchString(s) {
		return 0, ErrBadTimeSize
	}

	subs := fullRegexp.FindStringSubmatch(s)
	if len(subs) != 3 {
		return 0, ErrBadTimeSize
	}

	text := subs[1]
	unit := subs[2]

	switch {
	case unit != "":
		return time.ParseDuration(text + unit)
	case digitsOnly.MatchString(text):
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return 0, ErrBadTimeSize
		}
		n *= int64(time.Second)
		return time.Duration(n), nil
	default:
		n, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return 0, ErrBadTimeSize
		}
		n *= float64(time.Second)
		return time.Duration(n), nil
	}
}

func MustParse(s string) time.Duration {
	v, err := Parse(s)
	if err != nil {
		panic("parse timesize failed, err : " + err.Error())
	}
	return v
}
