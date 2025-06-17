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
