// Copyright 2019 The Bitalos-Stored author and other contributors.
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

package vectormap

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/zuoyebang/bitalostored/butils/md5hash"
)

var vh *kvHolder

func TestStoreAndLoadUint32(t *testing.T) {
	var holder [4]byte
	u := uint32(123456789)
	StoreUint32(unsafe.Pointer(&holder[0]), u)
	res := LoadUint32(unsafe.Pointer(&holder[0]))
	assert.Equal(t, u, res)
}

func TestKvHolder_SetGet(t *testing.T) {
	key := []byte("test")
	value := []byte("value")
	khash := md5hash.MD5(key)
	ki, fail := vh.set(khash, value)
	assert.Equal(t, false, fail)
	k, v := vh.getKV(ki)
	assert.Equal(t, khash, k)
	assert.Equal(t, value, v)

	value2 := []byte("valu")
	ki, fail = vh.update(ki, value2)
	assert.Equal(t, false, fail)
	k2, v2 := vh.getKV(ki)
	assert.Equal(t, khash, k2)
	assert.Equal(t, value2, v2)

	value3 := []byte("value__lt__8")
	ki, fail = vh.update(ki, value3)
	assert.Equal(t, false, fail)
	k3, v3 := vh.getKV(ki)
	assert.Equal(t, khash, k3)
	assert.Equal(t, value3, v3)

	lValue := make([]byte, 128)
	for i, _ := range lValue {
		lValue[i] = byte(i)
	}
	ki, fail = vh.update(ki, lValue)
	assert.Equal(t, false, fail)
	k, v = vh.getKV(ki)
	assert.Equal(t, khash, k)
	assert.Equal(t, lValue, v)

	lValue2 := make([]byte, 256)
	for i, _ := range lValue2 {
		lValue2[i] = byte(i)
	}
	ki, fail = vh.update(ki, lValue2)
	assert.Equal(t, false, fail)
	k, v = vh.getKV(ki)
	assert.Equal(t, khash, k)
	assert.Equal(t, lValue2, v)

	sValue := []byte("short_after_long")
	ki, fail = vh.update(ki, sValue)
	assert.Equal(t, false, fail)
	k, v = vh.getKV(ki)
	assert.Equal(t, khash, k)
	assert.Equal(t, sValue, v)
}

func TestMain(m *testing.M) {
	vh = newKVHolder(1 * MB)
	m.Run()
}
