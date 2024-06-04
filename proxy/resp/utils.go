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

package resp

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/proxy/internal/errn"
)

const (
	EX     ExpireType = "EX"
	PX     ExpireType = "PX"
	NoType ExpireType = ""
)

type ExpireType string

const (
	NX          SetCondition = "NX"
	XX          SetCondition = "XX"
	NoCondition SetCondition = ""
)

type SetCondition string

func ParseSetArgs(args [][]byte) (e ExpireType, t int64, c SetCondition, err error) {
	e = NoType
	c = NoCondition
	if len(args) <= 0 {
		return
	}
	for i := 0; i < len(args); {
		switch strings.ToUpper(unsafe2.String(args[i])) {
		case "EX":
			if i+1 >= len(args) {
				err = SyntaxErr
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
				err = SyntaxErr
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
			err = SyntaxErr
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

func UpperSlice(buf []byte) []byte {
	for i, r := range buf {
		if 'a' <= r && r <= 'z' {
			r -= 'a' - 'A'
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

func PackArgs(items ...interface{}) (args []interface{}) {
	for _, item := range items {
		v := reflect.ValueOf(item)
		switch v.Kind() {
		case reflect.Slice:
			if v.IsNil() {
				continue
			}
			for i := 0; i < v.Len(); i++ {
				args = append(args, v.Index(i).Interface())
			}
		case reflect.Map:
			if v.IsNil() {
				continue
			}
			for _, key := range v.MapKeys() {
				args = append(args, key.Interface(), v.MapIndex(key).Interface())
			}
		default:
			args = append(args, v.Interface())
		}
	}
	return args
}

func InterfaceByte(items [][]byte) []interface{} {
	var args = make([]interface{}, len(items))
	for i := 0; i < len(items); i++ {
		args[i] = items[i]
	}
	return args
}

func InterfaceString(items []string) []interface{} {
	var args = make([]interface{}, len(items))
	for i := 0; i < len(items); i++ {
		args[i] = items[i]
	}
	return args
}

func InterfaceByteSubKeys(key []byte, fields [][]byte) []interface{} {
	args := make([]interface{}, 0, 1+len(fields))
	args = append(args, key)
	for i := 0; i < len(fields); i++ {
		args = append(args, fields[i])
	}
	return args
}

func Int64(reply interface{}, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	switch reply := reply.(type) {
	case int64:
		return reply, nil
	case int:
		return int64(reply), nil
	case []byte:
		n, err := strconv.ParseInt(string(reply), 10, 64)
		return n, err
	case nil:
		return 0, errn.ErrReturnNil
	case bool:
		if reply == true {
			return 1, err
		} else {
			return 0, err
		}
	}
	return 0, fmt.Errorf("unexpected type for Int64, got type %T", reply)
}
