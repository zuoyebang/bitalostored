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

package extend

import "strconv"

func FormatInt(v int) string {
	return strconv.FormatInt(int64(v), 10)
}

func FormatInt8(v int8) string {
	return strconv.FormatInt(int64(v), 10)
}

func FormatInt16(v int16) string {
	return strconv.FormatInt(int64(v), 10)
}

func FormatInt32(v int32) string {
	return strconv.FormatInt(int64(v), 10)
}

func FormatInt64(v int64) string {
	return strconv.FormatInt(int64(v), 10)
}

func FormatUint(v uint) string {
	return strconv.FormatUint(uint64(v), 10)
}

func FormatUint8(v uint8) string {
	return strconv.FormatUint(uint64(v), 10)
}

func FormatUint16(v uint16) string {
	return strconv.FormatUint(uint64(v), 10)
}

func FormatUint32(v uint32) string {
	return strconv.FormatUint(uint64(v), 10)
}

func FormatUint64(v uint64) string {
	return strconv.FormatUint(uint64(v), 10)
}

func FormatFloat32(v float32) string {
	return strconv.FormatFloat(float64(v), 'f', 5, 32)
}

func FormatFloat64(v float64) string {
	return strconv.FormatFloat(v, 'f', 5, 64)
}

func FormatIntToSlice(v int) []byte {
	return strconv.AppendInt(nil, int64(v), 10)
}

func FormatInt8ToSlice(v int8) []byte {
	return strconv.AppendInt(nil, int64(v), 10)
}

func FormatInt16ToSlice(v int16) []byte {
	return strconv.AppendInt(nil, int64(v), 10)
}

func FormatInt32ToSlice(v int32) []byte {
	return strconv.AppendInt(nil, int64(v), 10)
}

func FormatInt64ToSlice(v int64) []byte {
	return strconv.AppendInt(nil, v, 10)
}

func FormatUintToSlice(v uint) []byte {
	return strconv.AppendUint(nil, uint64(v), 10)
}

func FormatUint8ToSlice(v uint8) []byte {
	return strconv.AppendUint(nil, uint64(v), 10)
}

func FormatUint16ToSlice(v uint16) []byte {
	return strconv.AppendUint(nil, uint64(v), 10)
}

func FormatUint32ToSlice(v uint32) []byte {
	return strconv.AppendUint(nil, uint64(v), 10)
}

func FormatUint64ToSlice(v uint64) []byte {
	return strconv.AppendUint(nil, uint64(v), 10)
}

func FormatFloat32ToSlice(v float32) []byte {
	return strconv.AppendFloat(nil, float64(v), 'f', -1, 32)
}

func FormatFloat64ToSlice(v float64) []byte {
	return strconv.AppendFloat(nil, float64(v), 'f', -1, 64)
}
