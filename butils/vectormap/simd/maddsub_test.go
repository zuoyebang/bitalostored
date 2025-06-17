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

//go:build noasm || !amd64

package simd

import (
	"reflect"
	"testing"
	"unsafe"
)

func TestFMSubs128epu8(t *testing.T) {
	c := [16]uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	d := [16]uint8{8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8}

	var out2 [16]uint8
	MSubs128epu8(unsafe.Pointer(&c), unsafe.Pointer(&d), unsafe.Pointer(&out2))
	res2 := [16]uint8{0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8}
	if !reflect.DeepEqual(out2, res2) {
		t.Errorf("MSub128(%v, %v, %v) = %v, want %v", c, d, out2, out2, res2)
	}
}

func TestFMSubs256epu16(t *testing.T) {
	c := [16]uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	d := [16]uint16{8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8}

	var out2 [16]uint16
	MSubs256epu16(unsafe.Pointer(&c), unsafe.Pointer(&d), unsafe.Pointer(&out2))
	res2 := [16]uint16{0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8}
	if !reflect.DeepEqual(out2, res2) {
		t.Errorf("MSub256(%v, %v, %v) = %v, want %v", c, d, out2, out2, res2)
	}
}
