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

//go:build !noasm && amd64

package simd

import "unsafe"

//go:noescape
func MAdd128epi8(a, b, out unsafe.Pointer)

//go:noescape
func MSub128epi8(a, b, out unsafe.Pointer)

//go:noescape
func MAdds128epi8(a, b, out unsafe.Pointer)

//go:noescape
func MSubs128epi8(a, b, out unsafe.Pointer)

//go:noescape
func MAdds128epu8(a, b, out unsafe.Pointer)

//go:noescape
func MSubs128epu8(a, b, out unsafe.Pointer)

//go:noescape
func MAdd256epi8(a, b, out unsafe.Pointer)

//go:noescape
func MSub256epi8(a, b, out unsafe.Pointer)

//go:noescape
func MAdds256epi8(a, b, out unsafe.Pointer)

//go:noescape
func MSubs256epi8(a, b, out unsafe.Pointer)

//go:noescape
func MAdds256epu8(a, b, out unsafe.Pointer)

//go:noescape
func MSubs256epu8(a, b, out unsafe.Pointer)

//go:noescape
func MSubs256epu16(a, b, out unsafe.Pointer)

func FMAdd128epi8(a, b, out *[16]int8) {
	MAdd128epi8(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&out[0]))
}

func FMSub128epi8(a, b, out *[16]int8) {
	MSub128epi8(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&out[0]))
}

func FMAdds128epi8(a, b, out *[16]int8) {
	MAdds128epi8(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&out[0]))
}

func FMSubs128epi8(a, b, out *[16]int8) {
	MSubs128epi8(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&out[0]))
}

func FMAdds128epu8(a, b, out *[16]uint8) {
	MAdds128epu8(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&out[0]))
}

func FMSubs128epu8(a, b, out *[16]uint8) {
	MSubs128epu8(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&out[0]))
}

func FMAdd256epi8(a, b, out *[32]int8) {
	MAdd256epi8(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&out[0]))
}

func FMSub256epi8(a, b, out *[32]int8) {
	MSub256epi8(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&out[0]))
}

func FMAdds256epi8(a, b, out *[32]int8) {
	MAdds256epi8(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&out[0]))
}

func FMSubs256epi8(a, b, out *[32]int8) {
	MSubs256epi8(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&out[0]))
}

func FMAdds256epu8(a, b, out *[32]uint8) {
	MAdds256epu8(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&out[0]))
}

func FMSubs256epu8(a, b, out *[32]uint8) {
	MSubs256epu8(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&out[0]))
}

func FMSubs256epu16(a, b, out *[16]uint16) {
	MSubs256epu16(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&out[0]))
}
