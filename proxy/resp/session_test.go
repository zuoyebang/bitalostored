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
	"bufio"
	"bytes"
	"errors"
	"testing"
)

func TestRespWriter(t *testing.T) {
	for _, fixture := range []struct {
		v interface{}
		e string
	}{
		{
			v: errors.New("Some error"),
			e: "-Some error\r\n",
		},
		{
			v: "Some status",
			e: "+Some status\r\n",
		},
		{
			v: int64(42),
			e: ":42\r\n",
		},
		{
			v: []byte("ultimate answer"),
			e: "$15\r\nultimate answer\r\n",
		},
		{
			v: []interface{}{[]byte("aaa"), []byte("bbb"), int64(42)},
			e: "*3\r\n$3\r\naaa\r\n$3\r\nbbb\r\n:42\r\n",
		},
		{
			v: [][]byte{[]byte("test"), nil, []byte("zzz")},
			e: "*3\r\n$4\r\ntest\r\n$-1\r\n$3\r\nzzz\r\n",
		},
		{
			v: nil,
			e: "$-1\r\n",
		},
		{
			v: []interface{}{[]interface{}{int64(1), int64(2), int64(3)}, []interface{}{"Foo", errors.New("Bar")}},
			e: "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n",
		},
	} {
		w := new(RespWriter)
		var b bytes.Buffer
		w.buff = bufio.NewWriter(&b)
		switch v := fixture.v.(type) {
		case error:
			w.WriteError(v)
		case string:
			w.WriteStatus(v)
		case int64:
			w.WriteInteger(v)
		case []byte:
			w.WriteBulk(v)
		case []interface{}:
			w.WriteArray(v)
		case [][]byte:
			w.WriteSliceArray(v)
		default:
			w.WriteBulk(b.Bytes())
		}
		w.Flush()
		if b.String() != fixture.e {
			t.Errorf("respWriter, actual: %q, expected: %q", b.String(), fixture.e)
		}
	}

}
