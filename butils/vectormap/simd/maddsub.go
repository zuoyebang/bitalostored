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

import "unsafe"

// a, b, out *[16]uint8
func MSubs128epu8(a, b, out unsafe.Pointer) {
	va := (*(*[16]uint8)(a))[:]
	vb := (*(*[16]uint8)(b))[:]
	vo := (*(*[16]uint8)(out))[:]
	for i := 0; i < 16; i++ {
		if va[i] < vb[i] {
			vo[i] = 0
			continue
		}
		vo[i] = va[i] - vb[i]
	}
}

// a, b, out *[16]uint16
func MSubs256epu16(a, b, out unsafe.Pointer) {
	va := (*(*[16]uint16)(a))[:]
	vb := (*(*[16]uint16)(b))[:]
	vo := (*(*[16]uint16)(out))[:]
	for i := 0; i < 16; i++ {
		if va[i] < vb[i] {
			vo[i] = 0
			continue
		}
		vo[i] = va[i] - vb[i]
	}
}
