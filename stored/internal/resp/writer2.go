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
	"sort"
	"strconv"
	"strings"
)

// Writer2 allows for writing RESP messages.
type Writer2 struct {
	b []byte
}

// WriteNull writes a null to the client
func (w *Writer2) WriteNull() {
	w.b = AppendNull(w.b)
}

// WriteArray writes an array header. You must then write additional
// sub-responses to the client to complete the response.
// For example to write two strings:
//
//	c.WriteArray(2)
//	c.WriteBulk("item 1")
//	c.WriteBulk("item 2")
func (w *Writer2) WriteArray(count int) {
	w.b = AppendArray(w.b, count)
}

// WriteBulk writes bulk bytes to the client.
func (w *Writer2) WriteBulk(bulk []byte) {
	w.b = AppendBulk(w.b, bulk)
}

// WriteBulkString writes a bulk string to the client.
func (w *Writer2) WriteBulkString(bulk string) {
	w.b = AppendBulkString(w.b, bulk)
}

// Buffer returns the unflushed buffer. This is a copy so changes
// to the resulting []byte will not affect the writer.
func (w *Writer2) Buffer() []byte {
	return append([]byte(nil), w.b...)
}

// SetBuffer replaces the unflushed buffer with new bytes.
func (w *Writer2) SetBuffer(raw []byte) {
	w.b = w.b[:0]
	w.b = append(w.b, raw...)
}

// Flush writes all unflushed Write* calls to the underlying writer.
func (w *Writer2) Flush() error {
	w.b = w.b[:0]
	return nil
}

// WriteError writes an error to the client.
func (w *Writer2) WriteError(msg string) {
	w.b = AppendError(w.b, msg)
}

// WriteString writes a string to the client.
func (w *Writer2) WriteString(msg string) {
	w.b = AppendString(w.b, msg)
}

// WriteInt writes an integer to the client.
func (w *Writer2) WriteInt(num int) {
	w.WriteInt64(int64(num))
}

// WriteInt64 writes a 64-bit signed integer to the client.
func (w *Writer2) WriteInt64(num int64) {
	w.b = AppendInt(w.b, num)
}

// WriteUint64 writes a 64-bit unsigned integer to the client.
func (w *Writer2) WriteUint64(num uint64) {
	w.b = AppendUint(w.b, num)
}

// WriteRaw writes raw data to the client.
func (w *Writer2) WriteRaw(data []byte) {
	w.b = append(w.b, data...)
}

// WriteAny writes any type to client.
//
//	nil             -> null
//	error           -> error (adds "ERR " when first word is not uppercase)
//	string          -> bulk-string
//	numbers         -> bulk-string
//	[]byte          -> bulk-string
//	bool            -> bulk-string ("0" or "1")
//	slice           -> array
//	map             -> array with key/value pairs
//	SimpleString    -> string
//	SimpleInt       -> integer
//	everything-else -> bulk-string representation using fmt.Sprint()
func (w *Writer2) WriteAny(v interface{}) {
	w.b = AppendAny(w.b, v)
}

// appendPrefix will append a "$3\r\n" style redis prefix for a message.
func appendPrefix(b []byte, c byte, n int64) []byte {
	if n >= 0 && n <= 9 {
		return append(b, c, byte('0'+n), '\r', '\n')
	}
	b = append(b, c)
	b = strconv.AppendInt(b, n, 10)
	return append(b, '\r', '\n')
}

// AppendUint appends a Redis protocol uint64 to the input bytes.
func AppendUint(b []byte, n uint64) []byte {
	b = append(b, ':')
	b = strconv.AppendUint(b, n, 10)
	return append(b, '\r', '\n')
}

// AppendInt appends a Redis protocol int64 to the input bytes.
func AppendInt(b []byte, n int64) []byte {
	return appendPrefix(b, ':', n)
}

// AppendArray appends a Redis protocol array to the input bytes.
func AppendArray(b []byte, n int) []byte {
	return appendPrefix(b, '*', int64(n))
}

// AppendBulk appends a Redis protocol bulk byte slice to the input bytes.
func AppendBulk(b []byte, bulk []byte) []byte {
	b = appendPrefix(b, '$', int64(len(bulk)))
	b = append(b, bulk...)
	return append(b, '\r', '\n')
}

// AppendBulkString appends a Redis protocol bulk string to the input bytes.
func AppendBulkString(b []byte, bulk string) []byte {
	b = appendPrefix(b, '$', int64(len(bulk)))
	b = append(b, bulk...)
	return append(b, '\r', '\n')
}

// AppendString appends a Redis protocol string to the input bytes.
func AppendString(b []byte, s string) []byte {
	b = append(b, '+')
	b = append(b, stripNewlines(s)...)
	return append(b, '\r', '\n')
}

// AppendError appends a Redis protocol error to the input bytes.
func AppendError(b []byte, s string) []byte {
	b = append(b, '-')
	b = append(b, stripNewlines(s)...)
	return append(b, '\r', '\n')
}

// AppendOK appends a Redis protocol OK to the input bytes.
func AppendOK(b []byte) []byte {
	return append(b, '+', 'O', 'K', '\r', '\n')
}
func stripNewlines(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] == '\r' || s[i] == '\n' {
			s = strings.Replace(s, "\r", " ", -1)
			s = strings.Replace(s, "\n", " ", -1)
			break
		}
	}
	return s
}

// AppendTile38 appends a Tile38 message to the input bytes.
func AppendTile38(b []byte, data []byte) []byte {
	b = append(b, '$')
	b = strconv.AppendInt(b, int64(len(data)), 10)
	b = append(b, ' ')
	b = append(b, data...)
	return append(b, '\r', '\n')
}

// AppendNull appends a Redis protocol null to the input bytes.
func AppendNull(b []byte) []byte {
	return append(b, '$', '-', '1', '\r', '\n')
}

// AppendBulkFloat appends a float64, as bulk bytes.
func AppendBulkFloat(dst []byte, f float64) []byte {
	return AppendBulk(dst, strconv.AppendFloat(nil, f, 'f', -1, 64))
}

// AppendBulkInt appends an int64, as bulk bytes.
func AppendBulkInt(dst []byte, x int64) []byte {
	return AppendBulk(dst, strconv.AppendInt(nil, x, 10))
}

// AppendBulkUint appends an uint64, as bulk bytes.
func AppendBulkUint(dst []byte, x uint64) []byte {
	return AppendBulk(dst, strconv.AppendUint(nil, x, 10))
}

func prefixERRIfNeeded(msg string) string {
	msg = strings.TrimSpace(msg)
	firstWord := strings.Split(msg, " ")[0]
	addERR := len(firstWord) == 0
	for i := 0; i < len(firstWord); i++ {
		if firstWord[i] < 'A' || firstWord[i] > 'Z' {
			addERR = true
			break
		}
	}
	if addERR {
		msg = strings.TrimSpace("ERR " + msg)
	}
	return msg
}

// SimpleString is for representing a non-bulk representation of a string
// from an *Any call.
type SimpleString string

// SimpleInt is for representing a non-bulk representation of a int
// from an *Any call.
type SimpleInt int

// Marshaler is the interface implemented by types that
// can marshal themselves into a Redis response type from an *Any call.
// The return value is not check for validity.
type Marshaler interface {
	MarshalRESP() []byte
}

// AppendAny appends any type to valid Redis type.
//
//	nil             -> null
//	error           -> error (adds "ERR " when first word is not uppercase)
//	string          -> bulk-string
//	numbers         -> bulk-string
//	[]byte          -> bulk-string
//	bool            -> bulk-string ("0" or "1")
//	slice           -> array
//	map             -> array with key/value pairs
//	SimpleString    -> string
//	SimpleInt       -> integer
//	Marshaler       -> raw bytes
//	everything-else -> bulk-string representation using fmt.Sprint()
func AppendAny(b []byte, v interface{}) []byte {
	switch v := v.(type) {
	case SimpleString:
		b = AppendString(b, string(v))
	case SimpleInt:
		b = AppendInt(b, int64(v))
	case nil:
		b = AppendNull(b)
	case error:
		b = AppendError(b, prefixERRIfNeeded(v.Error()))
	case string:
		b = AppendBulkString(b, v)
	case []byte:
		b = AppendBulk(b, v)
	case bool:
		if v {
			b = AppendBulkString(b, "1")
		} else {
			b = AppendBulkString(b, "0")
		}
	case int:
		b = AppendBulkInt(b, int64(v))
	case int8:
		b = AppendBulkInt(b, int64(v))
	case int16:
		b = AppendBulkInt(b, int64(v))
	case int32:
		b = AppendBulkInt(b, int64(v))
	case int64:
		b = AppendBulkInt(b, v)
	case uint:
		b = AppendBulkUint(b, uint64(v))
	case uint8:
		b = AppendBulkUint(b, uint64(v))
	case uint16:
		b = AppendBulkUint(b, uint64(v))
	case uint32:
		b = AppendBulkUint(b, uint64(v))
	case uint64:
		b = AppendBulkUint(b, v)
	case float32:
		b = AppendBulkFloat(b, float64(v))
	case float64:
		b = AppendBulkFloat(b, float64(v))
	case Marshaler:
		b = append(b, v.MarshalRESP()...)
	default:
		vv := reflect.ValueOf(v)
		switch vv.Kind() {
		case reflect.Slice:
			n := vv.Len()
			b = AppendArray(b, n)
			for i := 0; i < n; i++ {
				b = AppendAny(b, vv.Index(i).Interface())
			}
		case reflect.Map:
			n := vv.Len()
			b = AppendArray(b, n*2)
			var i int
			var strKey bool
			var strsKeyItems []strKeyItem

			iter := vv.MapRange()
			for iter.Next() {
				key := iter.Key().Interface()
				if i == 0 {
					if _, ok := key.(string); ok {
						strKey = true
						strsKeyItems = make([]strKeyItem, n)
					}
				}
				if strKey {
					strsKeyItems[i] = strKeyItem{
						key.(string), iter.Value().Interface(),
					}
				} else {
					b = AppendAny(b, key)
					b = AppendAny(b, iter.Value().Interface())
				}
				i++
			}
			if strKey {
				sort.Slice(strsKeyItems, func(i, j int) bool {
					return strsKeyItems[i].key < strsKeyItems[j].key
				})
				for _, item := range strsKeyItems {
					b = AppendBulkString(b, item.key)
					b = AppendAny(b, item.value)
				}
			}
		default:
			b = AppendBulkString(b, fmt.Sprint(v))
		}
	}
	return b
}

type strKeyItem struct {
	key   string
	value interface{}
}
