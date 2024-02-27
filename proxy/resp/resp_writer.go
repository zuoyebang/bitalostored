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

package resp

import (
	"bufio"
	"io"
	"net"
	"strconv"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
)

var (
	Delims = []byte("\r\n")

	NullBulk  = []byte("-1")
	NullArray = []byte("-1")

	ReplyOK     = "OK"
	ReplyPONG   = "PONG"
	ReplyQUEUED = "QUEUED"
)

type RespWriter struct {
	buff *bufio.Writer
}

func NewRespWriter(conn net.Conn, size int) *RespWriter {
	w := new(RespWriter)
	w.buff = bufio.NewWriterSize(conn, size)
	return w
}

func (w *RespWriter) WriteError(err error) {
	w.buff.Write([]byte{'-'})
	if err != nil {
		w.buff.Write(unsafe2.ByteSlice(err.Error()))
	}
	w.buff.Write(Delims)
}

func (w *RespWriter) WriteStatus(status string) {
	w.buff.WriteByte('+')
	w.buff.Write(unsafe2.ByteSlice(status))
	w.buff.Write(Delims)
}

func (w *RespWriter) WriteInteger(n int64) {
	w.buff.WriteByte(':')
	w.buff.Write(extend.FormatInt64ToSlice(n))
	w.buff.Write(Delims)
}

func (w *RespWriter) WriteBulk(b []byte) {
	w.buff.WriteByte('$')
	if b == nil {
		w.buff.Write(NullBulk)
	} else {
		w.buff.Write(unsafe2.ByteSlice(strconv.Itoa(len(b))))
		w.buff.Write(Delims)
		w.buff.Write(b)
	}

	w.buff.Write(Delims)
}

func (w *RespWriter) WriteArray(lst []interface{}) {
	w.buff.WriteByte('*')
	if lst == nil {
		w.buff.Write(NullArray)
		w.buff.Write(Delims)
	} else {
		w.buff.Write(unsafe2.ByteSlice(strconv.Itoa(len(lst))))
		w.buff.Write(Delims)

		for i := 0; i < len(lst); i++ {
			switch v := lst[i].(type) {
			case []interface{}:
				w.WriteArray(v)
			case [][]byte:
				w.WriteSliceArray(v)
			case []byte:
				w.WriteBulk(v)
			case nil:
				w.WriteBulk(nil)
			case int64:
				w.WriteInteger(v)
			case string:
				w.WriteStatus(v)
			case error:
				w.WriteError(v)
			}
		}
	}
}

func (w *RespWriter) WriteSliceArray(lst [][]byte) {
	w.buff.WriteByte('*')
	if lst == nil {
		w.buff.Write(NullArray)
		w.buff.Write(Delims)
	} else {
		w.buff.Write(unsafe2.ByteSlice(strconv.Itoa(len(lst))))
		w.buff.Write(Delims)

		for i := 0; i < len(lst); i++ {
			w.WriteBulk(lst[i])
		}
	}
}

func (w *RespWriter) WriteFVPairArray(lst [][]byte) {
	w.WriteSliceArray(lst)
}

func (w *RespWriter) WriteScorePairArray(lst [][]byte, withScores bool) {
	w.WriteSliceArray(lst)
}

func (w *RespWriter) WriteBulkFrom(n int64, rb io.Reader) {
	w.buff.WriteByte('$')
	w.buff.Write(unsafe2.ByteSlice(strconv.FormatInt(n, 10)))
	w.buff.Write(Delims)

	io.Copy(w.buff, rb)
	w.buff.Write(Delims)
}

func (w *RespWriter) Flush() {
	w.buff.Flush()
}
