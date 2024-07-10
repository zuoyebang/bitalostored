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

	"github.com/zuoyebang/bitalostored/butils/deepcopy"
	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

const writerBufferSize = 8 << 10

var (
	respArray byte = '*'
	respInt   byte = ':'
	respErr   byte = '-'
	respMutil byte = '$'
	respSinge byte = '+'

	respInternalFieldPair  byte = 'F'
	respInternalScorePair  byte = 'S'
	respInternalFVPair     byte = 'V'
	respInternalSliceArray byte = 's'
	respInternalArray      byte = 'a'

	Delims    = []byte("\r\n")
	NullBulk  = []byte("-1")
	NullArray = []byte("0")

	ReplyOK     = "OK"
	ReplyPONG   = "PONG"
	ReplyQUEUED = "QUEUED"
)

type Writer struct {
	Buf    *bytes.Buffer
	Cached bool
	Resps  []RespOuput
}

type RespOuput struct {
	Type       byte
	WithScores bool
	Output     interface{}
}

func NewWriter() *Writer {
	w := &Writer{
		Buf: bytes.NewBuffer(make([]byte, 0, writerBufferSize)),
	}
	return w
}

func (w *Writer) SetCached() {
	w.Cached = true
}

func (w *Writer) UnsetCached() {
	w.Cached = false
}

func (w *Writer) FlushCached() {
	w.Buf.WriteByte(respArray)
	w.Buf.Write(unsafe2.ByteSlice(strconv.Itoa(len(w.Resps))))
	w.Buf.Write(Delims)

	for _, resp := range w.Resps {
		switch resp.Type {
		case respErr:
			out := resp.Output.(error)
			w.WriteError(out)
		case respSinge:
			out := resp.Output.(string)
			w.WriteStatus(out)
		case respInt:
			out := resp.Output.(int64)
			w.WriteInteger(out)
		case respMutil:
			if resp.Output == nil {
				w.WriteBulk(nil)
			} else {
				out := resp.Output.([]byte)
				w.WriteBulk(out)
			}
		case respInternalSliceArray:
			if resp.Output == nil {
				w.WriteSliceArray(nil)
			} else {
				out := resp.Output.([][]byte)
				w.WriteSliceArray(out)
			}
		case respInternalArray:
			if resp.Output == nil {
				w.WriteArray(nil)
			} else {
				out := resp.Output.([]interface{})
				w.WriteArray(out)
			}
		case respInternalFVPair:
			if resp.Output == nil {
				w.WriteFVPairArray(nil)
			} else {
				out := resp.Output.([]btools.FVPair)
				w.WriteFVPairArray(out)
			}
		case respInternalFieldPair:
			if resp.Output == nil {
				w.WriteFieldPairArray(nil)
			} else {
				out := resp.Output.([]btools.FieldPair)
				w.WriteFieldPairArray(out)
			}
		case respInternalScorePair:
			if resp.Output == nil {
				w.WriteScorePairArray(nil, resp.WithScores)
			} else {
				out := resp.Output.([]btools.ScorePair)
				w.WriteScorePairArray(out, resp.WithScores)
			}
		}
	}
	w.Resps = w.Resps[:0]
}

func (w *Writer) WriteError(err error) {
	if w.Cached {
		w.Resps = append(w.Resps, RespOuput{Type: respErr, Output: err})
		return
	}
	w.Buf.WriteByte(respErr)
	if err != nil {
		w.Buf.Write(unsafe2.ByteSlice(err.Error()))
	}
	w.Buf.Write(Delims)
}

func (w *Writer) WriteStatus(status string) {
	if w.Cached {
		w.Resps = append(w.Resps, RespOuput{Type: respSinge, Output: status})
		return
	}
	w.Buf.WriteByte(respSinge)
	w.Buf.Write(unsafe2.ByteSlice(status))
	w.Buf.Write(Delims)
}

func (w *Writer) WriteInteger(n int64) {
	if w.Cached {
		w.Resps = append(w.Resps, RespOuput{Type: respInt, Output: n})
		return
	}
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
	if w.Cached {
		if b == nil {
			w.Resps = append(w.Resps, RespOuput{Type: respMutil, Output: nil})
		} else {
			bc := make([]byte, 0, len(b))
			bc = append(bc, b...)
			w.Resps = append(w.Resps, RespOuput{Type: respMutil, Output: bc})
		}
		return
	}
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

func (w *Writer) WriteBulkMulti(bs ...[]byte) {
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
	if w.Cached {
		if lst == nil {
			w.Resps = append(w.Resps, RespOuput{Type: respInternalArray, Output: nil})
		} else {
			w.Resps = append(w.Resps, RespOuput{Type: respInternalArray, Output: deepcopy.Copy(lst)})
		}
		return
	}
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
	if w.Cached {
		if lst == nil {
			w.Resps = append(w.Resps, RespOuput{Type: respInternalSliceArray, Output: nil})
		} else {
			w.Resps = append(w.Resps, RespOuput{Type: respInternalSliceArray, Output: deepcopy.Copy(lst)})
		}
		return
	}
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
	if w.Cached {
		if lst == nil {
			w.Resps = append(w.Resps, RespOuput{Type: respInternalFVPair, Output: nil})
		} else {
			w.Resps = append(w.Resps, RespOuput{Type: respInternalFVPair, Output: deepcopy.Copy(lst)})
		}
		return
	}
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

func (w *Writer) WriteFieldPairArray(lst []btools.FieldPair) {
	if w.Cached {
		if lst == nil {
			w.Resps = append(w.Resps, RespOuput{Type: respInternalFieldPair, Output: nil})
		} else {
			w.Resps = append(w.Resps, RespOuput{Type: respInternalFieldPair, Output: deepcopy.Copy(lst)})
		}
		return
	}
	w.Buf.WriteByte(respArray)

	if lst == nil {
		w.Buf.Write(NullArray)
		w.Buf.Write(Delims)
	} else {
		w.Buf.Write(unsafe2.ByteSlice(strconv.Itoa(len(lst) * 2)))
		w.Buf.Write(Delims)

		for i := 0; i < len(lst); i++ {
			w.WriteBulkMulti(lst[i].Prefix, lst[i].Suffix)
		}
	}
}

func (w *Writer) WriteScorePairArray(lst []btools.ScorePair, withScores bool) {
	if w.Cached {
		if lst == nil {
			w.Resps = append(w.Resps, RespOuput{Type: respInternalScorePair, WithScores: withScores, Output: nil})
		} else {
			w.Resps = append(w.Resps, RespOuput{Type: respInternalScorePair, WithScores: withScores, Output: deepcopy.Copy(lst)})
		}
		return
	}
	w.Buf.WriteByte(respArray)

	if lst == nil {
		w.Buf.Write(NullArray)
		w.Buf.Write(Delims)
	} else {
		if withScores {
			w.Buf.Write(unsafe2.ByteSlice(strconv.Itoa(len(lst) * 2)))
			w.Buf.Write(Delims)

		} else {
			w.Buf.Write(unsafe2.ByteSlice(strconv.Itoa(len(lst))))
			w.Buf.Write(Delims)
		}

		for i := 0; i < len(lst); i++ {
			w.WriteBulk(lst[i].Member)

			if withScores {
				w.WriteBulk(extend.FormatFloat64ToSlice(lst[i].Score))
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
	w.Buf.Reset()
}

func (w *Writer) FlushToWriterIO(writer io.Writer) (int, error) {
	defer w.Buf.Reset()
	return writer.Write(w.Buf.Bytes())
}
