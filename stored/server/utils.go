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

package server

import (
	"bufio"
	"errors"
	"strconv"
	"strings"

	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
)

const (
	EX      ExpireType = "EX"
	PX      ExpireType = "PX"
	NO_TYPE ExpireType = ""
)

const (
	NX           SetCondition = "NX"
	XX           SetCondition = "XX"
	NO_CONDITION SetCondition = ""
)

var (
	BEFORE = []byte("before")
	AFTER  = []byte("after")
)

type ExpireType string
type SetCondition string

func ParseSetArgs(args [][]byte) (e ExpireType, t int64, c SetCondition, err error) {
	e = NO_TYPE
	c = NO_CONDITION
	if len(args) <= 0 {
		return
	}
	for i := 0; i < len(args); {
		switch strings.ToUpper(unsafe2.String(args[i])) {
		case "EX":
			if i+1 >= len(args) {
				err = errn.ErrSyntax
				return
			}

			e = EX
			t, err = strconv.ParseInt(unsafe2.String(args[i+1]), 10, 64)
			if err != nil {
				return
			}
			i++
		case "PX":
			if i+1 >= len(args) {
				err = errn.ErrSyntax
				return
			}

			e = PX
			t, err = strconv.ParseInt(unsafe2.String(args[i+1]), 10, 64)
			if err != nil {
				return
			}
			i++
		case "NX":
			c = NX
		case "XX":
			c = XX
		default:
			err = errn.ErrSyntax
			return
		}
		i++
	}
	return
}

func LowerSlice(buf []byte) []byte {
	for i, r := range buf {
		if 'A' <= r && r <= 'Z' {
			r += 'a' - 'A'
		}

		buf[i] = r
	}
	return buf
}

func StringSlice(b [][]byte) []string {
	res := make([]string, 0, len(b))
	for _, value := range b {
		res = append(res, unsafe2.String(value))
	}
	return res
}

func ParseReply(rd *bufio.Reader) (interface{}, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 {
		return nil, errn.ErrProtocol
	}

	switch line[0] {
	default:
		return nil, errn.ErrProtocol
	case '+':
		return line[1 : len(line)-2], nil
	case '-':
		return nil, errors.New(line[1 : len(line)-2])
	case ':':
		v := line[1 : len(line)-2]
		if v == "" {
			return 0, nil
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, errn.ErrProtocol
		}
		return n, nil
	case '$':
		length, err := strconv.Atoi(line[1 : len(line)-2])
		if err != nil {
			return "", err
		}
		if length < 0 {
			return nil, nil
		}
		var (
			buf = make([]byte, length+2)
			pos = 0
		)
		for pos < length+2 {
			n, err := rd.Read(buf[pos:])
			if err != nil {
				return "", err
			}
			pos += n
		}
		return string(buf[:length]), nil
	case '*':
		l, err := strconv.Atoi(line[1 : len(line)-2])
		if err != nil {
			return nil, errn.ErrProtocol
		}
		var fields []interface{}
		for ; l > 0; l-- {
			s, err := ParseReply(rd)
			if err != nil {
				return nil, err
			}
			fields = append(fields, s)
		}
		return fields, nil
	}
}
