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

import (
	"reflect"
	"testing"
)

func TestFMAdd128epi8(t *testing.T) {
	a := [16]int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	b := [16]int8{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}
	var out [16]int8
	FMAdd128epi8(&a, &b, &out)
	res := [16]int8{15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15}
	if !reflect.DeepEqual(out, res) {
		t.Errorf("MAdd128(%v, %v, %v) = %v, want %v", a, b, out, out, res)
	}

	c := [16]int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	d := [16]int8{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}
	res2 := [16]int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	var out2 [16]int8
	FMAdd128epi8(&c, &d, &out2)
	if !reflect.DeepEqual(out2, res2) {
		t.Errorf("MAdd128(%v, %v, %v) = %v, want %v", c, d, out2, out2, res2)
	}
}

func TestFMSub128epi8(t *testing.T) {
	c := [16]int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	d := [16]int8{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}

	var out2 [16]int8
	FMSub128epi8(&c, &d, &out2)
	res2 := [16]int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	if !reflect.DeepEqual(out2, res2) {
		t.Errorf("MSub128(%v, %v, %v) = %v, want %v", c, d, out2, out2, res2)
	}
}

func TestFMAdds128epi8(t *testing.T) {
	a := [16]int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	b := [16]int8{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 127}
	var out [16]int8
	FMAdds128epi8(&a, &b, &out)
	res := [16]int8{15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 127}
	if !reflect.DeepEqual(out, res) {
		t.Errorf("MAdd128(%v, %v, %v) = %v, want %v", a, b, out, out, res)
	}
}

func TestFMSubs128epi8(t *testing.T) {
	c := [16]int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, -125, -126, -127, -128, -128}
	d := [16]int8{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}

	var out2 [16]int8
	FMSubs128epi8(&c, &d, &out2)
	res2 := [16]int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, -126, -127, -128, -128, -128}
	if !reflect.DeepEqual(out2, res2) {
		t.Errorf("MSub128(%v, %v, %v) = %v, want %v", c, d, out2, out2, res2)
	}
}

func TestFMAdds128epu8(t *testing.T) {
	a := [16]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	b := [16]uint8{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 254, 255}
	var out [16]uint8
	FMAdds128epu8(&a, &b, &out)
	res := [16]uint8{15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 255, 255}
	if !reflect.DeepEqual(out, res) {
		t.Errorf("MAdd128(%v, %v, %v) = %v, want %v", a, b, out, out, res)
	}
}

func TestFMSubs128epu8(t *testing.T) {
	c := [16]uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	d := [16]uint8{8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8}

	var out2 [16]uint8
	FMSubs128epu8(&c, &d, &out2)
	res2 := [16]uint8{0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8}
	if !reflect.DeepEqual(out2, res2) {
		t.Errorf("MSub128(%v, %v, %v) = %v, want %v", c, d, out2, out2, res2)
	}
}

func TestFMAdd256epi8(t *testing.T) {
	a := [32]int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	b := [32]int8{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}
	var out [32]int8
	FMAdd256epi8(&a, &b, &out)
	res := [32]int8{15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15}
	if !reflect.DeepEqual(out, res) {
		t.Errorf("MAdd128(%v, %v, %v) = %v, want %v", a, b, out, out, res)
	}

	c := [32]int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	d := [32]int8{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}
	res2 := [32]int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	var out2 [32]int8
	FMAdd256epi8(&c, &d, &out2)
	if !reflect.DeepEqual(out2, res2) {
		t.Errorf("MAdd256(%v, %v, %v) = %v, want %v", c, d, out2, out2, res2)
	}
}

func TestFMSub256epi8(t *testing.T) {
	c := [32]int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	d := [32]int8{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}

	var out2 [32]int8
	FMSub256epi8(&c, &d, &out2)
	res2 := [32]int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	if !reflect.DeepEqual(out2, res2) {
		t.Errorf("MSub256(%v, %v, %v) = %v, want %v", c, d, out2, out2, res2)
	}
}

func TestFMAdds256epi8(t *testing.T) {
	a := [32]int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	b := [32]int8{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 127}
	var out [32]int8
	FMAdds256epi8(&a, &b, &out)
	res := [32]int8{15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 127}
	if !reflect.DeepEqual(out, res) {
		t.Errorf("MAdd256(%v, %v, %v) = %v, want %v", a, b, out, out, res)
	}
}

func TestFMSubs256epi8(t *testing.T) {
	c := [32]int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, -125, -126, -127, -128, -128}
	d := [32]int8{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}

	var out2 [32]int8
	FMSubs256epi8(&c, &d, &out2)
	res2 := [32]int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, -126, -127, -128, -128, -128}
	if !reflect.DeepEqual(out2, res2) {
		t.Errorf("MSub256(%v, %v, %v) = %v, want %v", c, d, out2, out2, res2)
	}
}

func TestFMAdds256epu8(t *testing.T) {
	a := [32]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	b := [32]uint8{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 254, 255}
	var out [32]uint8
	FMAdds256epu8(&a, &b, &out)
	res := [32]uint8{15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 255, 255}
	if !reflect.DeepEqual(out, res) {
		t.Errorf("MAdd256(%v, %v, %v) = %v, want %v", a, b, out, out, res)
	}
}

func TestFMSubs256epu8(t *testing.T) {
	c := [32]uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	d := [32]uint8{8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8}

	var out2 [32]uint8
	FMSubs256epu8(&c, &d, &out2)
	res2 := [32]uint8{0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8}
	if !reflect.DeepEqual(out2, res2) {
		t.Errorf("MSub256(%v, %v, %v) = %v, want %v", c, d, out2, out2, res2)
	}
}

func TestFMSubs256epu16(t *testing.T) {
	c := [16]uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	d := [16]uint16{8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8}

	var out2 [16]uint16
	FMSubs256epu16(&c, &d, &out2)
	res2 := [16]uint16{0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8}
	if !reflect.DeepEqual(out2, res2) {
		t.Errorf("MSub256(%v, %v, %v) = %v, want %v", c, d, out2, out2, res2)
	}
}
