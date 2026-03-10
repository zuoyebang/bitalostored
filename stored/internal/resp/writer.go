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
	"bytes"
	"io"
	"strconv"

	"github.com/zuoyebang/bitalostored/stored/engine/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/log"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
)

const writerBufferSize = 8 << 10

var (
	respArray byte = '*'
	respInt   byte = ':'
	respErr   byte = '-'
	respMutil byte = '$'
	respSinge byte = '+'

	Delims    = []byte("\r\n")
	NullBulk  = []byte("-1")
	NullArray = []byte("0")

	ReplyOK     = "OK"
	ReplyPONG   = "PONG"
	ReplyQUEUED = "QUEUED"
)

type Writer struct {
	Header *bytes.Buffer
	Buf    *bytes.Buffer
}

func NewWriter() *Writer {
	w := &Writer{
		Header: bytes.NewBuffer(make([]byte, 0, 32)),
		Buf:    bytes.NewBuffer(make([]byte, 0, writerBufferSize)),
	}
	return w
}

func (w *Writer) WriteHeader(n int) {
	w.Header.WriteByte(respArray)
	w.Header.Write(unsafe2.ByteSlice(strconv.Itoa(n)))
	w.Header.Write(Delims)
}

func (w *Writer) WriteError(err error) {
	w.Buf.WriteByte(respErr)
	if err != nil {
		w.Buf.Write(unsafe2.ByteSlice(err.Error()))
	}
	w.Buf.Write(Delims)
}

func (w *Writer) WriteStatus(status string) {
	w.Buf.WriteByte(respSinge)
	w.Buf.Write(unsafe2.ByteSlice(status))
	w.Buf.Write(Delims)
}

func (w *Writer) WriteInteger(n int64) {
	w.Buf.WriteByte(respInt)
	w.Buf.Write(extend.FormatInt64ToSlice(n))
	w.Buf.Write(Delims)
}

func (w *Writer) WriteLen(n int) {
	w.Buf.WriteByte(respArray)
	w.Buf.Write(unsafe2.ByteSlice(strconv.Itoa(n)))
	w.Buf.Write(Delims)
}

func (w *Writer) WriteBulk(b []byte) {
	w.Buf.WriteByte(respMutil)
	if b == nil {
		w.Buf.Write(NullBulk)
	} else {
		w.Buf.Write(unsafe2.ByteSlice(strconv.Itoa(len(b))))
		w.Buf.Write(Delims)
		w.Buf.Write(b)
	}
	w.Buf.Write(Delims)
}

func (w *Writer) WriteBulks(bs ...[]byte) {
	w.Buf.WriteByte(respMutil)

	blen := 0
	for i := range bs {
		blen += len(bs[i])
	}

	if blen == 0 {
		w.Buf.Write(NullBulk)
	} else {
		w.Buf.Write(unsafe2.ByteSlice(strconv.Itoa(blen)))
		w.Buf.Write(Delims)
		for i := range bs {
			if len(bs[i]) > 0 {
				w.Buf.Write(bs[i])
			}
		}
	}

	w.Buf.Write(Delims)
}

func (w *Writer) WriteArray(lst []interface{}) {
	w.Buf.WriteByte(respArray)

	if lst == nil {
		w.Buf.Write(NullBulk)
		w.Buf.Write(Delims)
	} else {
		w.Buf.Write(unsafe2.ByteSlice(strconv.Itoa(len(lst))))
		w.Buf.Write(Delims)

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
			default:
				log.Errorf("invalid array type %T %v", lst[i], v)
			}
		}
	}
}

func (w *Writer) WriteSliceArray(lst [][]byte) {
	w.Buf.WriteByte(respArray)

	if lst == nil {
		w.Buf.Write(NullArray)
		w.Buf.Write(Delims)
	} else {
		w.Buf.Write(unsafe2.ByteSlice(strconv.Itoa(len(lst))))
		w.Buf.Write(Delims)

		for i := 0; i < len(lst); i++ {
			w.WriteBulk(lst[i])
		}
	}
}

func (w *Writer) WriteFVPairArray(lst []btools.FVPair) {
	w.Buf.WriteByte(respArray)

	if lst == nil {
		w.Buf.Write(NullArray)
		w.Buf.Write(Delims)
	} else {
		w.Buf.Write(unsafe2.ByteSlice(strconv.Itoa(len(lst) * 2)))
		w.Buf.Write(Delims)

		for i := 0; i < len(lst); i++ {
			w.WriteBulk(lst[i].Field)
			w.WriteBulk(lst[i].Value)
		}
	}
}

func (w *Writer) WriteFieldPair(f btools.FieldPair) {
	w.WriteBulks(f.Prefix, f.Suffix)
}

func (w *Writer) WriteFieldPairArray(fs []btools.FieldPair) {
	w.Buf.WriteByte(respArray)

	fsLen := len(fs)
	if fsLen == 0 {
		w.Buf.Write(NullArray)
		w.Buf.Write(Delims)
	} else {
		w.Buf.Write(unsafe2.ByteSlice(strconv.Itoa(fsLen * 2)))
		w.Buf.Write(Delims)
		for i := 0; i < fsLen; i++ {
			w.WriteFieldPair(fs[i])
		}
	}
}

func (w *Writer) WriteZsetPairArray(zs []btools.ZsetPair, withScores bool) {
	w.Buf.WriteByte(respArray)

	zsLen := len(zs)
	if zsLen == 0 {
		w.Buf.Write(NullArray)
		w.Buf.Write(Delims)
	} else {
		if withScores {
			w.Buf.Write(unsafe2.ByteSlice(strconv.Itoa(zsLen * 2)))
			w.Buf.Write(Delims)
		} else {
			w.Buf.Write(unsafe2.ByteSlice(strconv.Itoa(zsLen)))
			w.Buf.Write(Delims)
		}

		for i := 0; i < zsLen; i++ {
			w.WriteBulks(zs[i].Prefix, zs[i].Suffix)
			if withScores {
				w.WriteBulk(extend.FormatFloat64ToSlice(zs[i].Score))
			}
		}
	}
}

func (w *Writer) WriteScorePairArray(zs []btools.ScorePair, withScores bool) {
	w.Buf.WriteByte(respArray)

	zsLen := len(zs)
	if zsLen == 0 {
		w.Buf.Write(NullArray)
		w.Buf.Write(Delims)
	} else {
		if withScores {
			w.Buf.Write(unsafe2.ByteSlice(strconv.Itoa(zsLen * 2)))
			w.Buf.Write(Delims)
		} else {
			w.Buf.Write(unsafe2.ByteSlice(strconv.Itoa(zsLen)))
			w.Buf.Write(Delims)
		}

		for i := 0; i < zsLen; i++ {
			w.WriteBulk(zs[i].Member)
			if withScores {
				w.WriteBulk(extend.FormatFloat64ToSlice(zs[i].Score))
			}
		}
	}
}

func (w *Writer) WriteBytes(args ...[]byte) {
	for _, v := range args {
		w.Buf.Write(v)
	}
}

func (w *Writer) Bytes() []byte {
	return w.Buf.Bytes()
}

func (w *Writer) Reset() {
	if w.Header.Len() > 0 {
		w.Header.Reset()
	}
	w.Buf.Reset()
}

func (w *Writer) FlushToWriterIO(writer io.Writer) (int, error) {
	defer w.Reset()

	if w.Header.Len() > 0 {
		n, err := writer.Write(w.Header.Bytes())
		if err != nil {
			return n, err
		}
	}

	return writer.Write(w.Buf.Bytes())
}
