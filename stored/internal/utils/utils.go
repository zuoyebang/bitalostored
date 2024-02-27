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

package utils

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
)

const TotalSlot uint32 = 1024
const TxParallelLimit int32 = 200

func BoolToString(flag bool) string {
	if flag {
		return "true"
	}
	return "false"
}

func ExtractHashTag(key []byte) []byte {
	if beg := bytes.IndexByte(key, '{'); beg >= 0 {
		if end := bytes.IndexByte(key[beg+1:], '}'); end >= 0 {
			key = key[beg+1 : beg+1+end]
		}
	}
	return key
}

func GetHashTagFnv(key []byte) uint32 {
	hashTag := ExtractHashTag(key)
	khash := hash.Fnv32(hashTag)
	return khash
}

func StringSliceToByteSlice(ss []string) (ret [][]byte) {
	ret = make([][]byte, len(ss))
	for i, s := range ss {
		ret[i] = unsafe2.ByteSlice(s)
	}
	return
}

func Sha1Hex(s string) string {
	h := sha1.New()
	_, _ = io.WriteString(h, s)
	return hex.EncodeToString(h.Sum(nil))
}

func ByteToInt64(v []byte) (int64, error) {
	if v == nil {
		return 0, nil
	}
	return extend.ParseInt64(unsafe2.String(v))
}

func ByteToFloat64(v []byte) (float64, error) {
	if v == nil {
		return 0, nil
	}
	return extend.ParseFloat64(unsafe2.String(v))
}

func GetSlotId(khash uint32) uint16 {
	return uint16(khash % TotalSlot)
}

func GetKeySlotId(key []byte) uint32 {
	return hash.Fnv32(key) % TotalSlot
}

func GetCurrentTimeString() string {
	return time.Now().Format(time.DateTime)
}

func GetLocalIp() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		if ipNet, ok := address.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String()
			}
		}
	}
	return ""
}

func FirstError(err1 error, err2 error) error {
	if err1 != nil {
		return err1
	}
	return err2
}

func AppendInfoUint(buf []byte, key string, value uint64) []byte {
	buf = append(buf, key...)
	buf = strconv.AppendUint(buf, value, 10)
	buf = append(buf, '\n')
	return buf
}

func AppendInfoInt(buf []byte, key string, value int64) []byte {
	buf = append(buf, key...)
	buf = strconv.AppendInt(buf, value, 10)
	buf = append(buf, '\n')
	return buf
}

func AppendInfoString(buf []byte, key string, value string) []byte {
	buf = append(buf, key...)
	buf = append(buf, value...)
	buf = append(buf, '\n')
	return buf
}

func AppendInfoFloat(buf []byte, key string, value float64, prec int) []byte {
	buf = append(buf, key...)
	buf = strconv.AppendFloat(buf, value, 'f', prec, 64)
	buf = append(buf, '\n')
	return buf
}
