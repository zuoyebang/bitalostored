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

package runes

func Index(s, needle []rune) int {
	ls, ln := len(s), len(needle)

	switch {
	case ln == 0:
		return 0
	case ln == 1:
		return IndexRune(s, needle[0])
	case ln == ls:
		if Equal(s, needle) {
			return 0
		}
		return -1
	case ln > ls:
		return -1
	}

head:
	for i := 0; i < ls && ls-i >= ln; i++ {
		for y := 0; y < ln; y++ {
			if s[i+y] != needle[y] {
				continue head
			}
		}

		return i
	}

	return -1
}

func LastIndex(s, needle []rune) int {
	ls, ln := len(s), len(needle)

	switch {
	case ln == 0:
		if ls == 0 {
			return 0
		}
		return ls
	case ln == 1:
		return IndexLastRune(s, needle[0])
	case ln == ls:
		if Equal(s, needle) {
			return 0
		}
		return -1
	case ln > ls:
		return -1
	}

head:
	for i := ls - 1; i >= 0 && i >= ln; i-- {
		for y := ln - 1; y >= 0; y-- {
			if s[i-(ln-y-1)] != needle[y] {
				continue head
			}
		}

		return i - ln + 1
	}

	return -1
}

func IndexAny(s, chars []rune) int {
	if len(chars) > 0 {
		for i, c := range s {
			for _, m := range chars {
				if c == m {
					return i
				}
			}
		}
	}
	return -1
}

func Contains(s, needle []rune) bool {
	return Index(s, needle) >= 0
}

func Max(s []rune) (max rune) {
	for _, r := range s {
		if r > max {
			max = r
		}
	}

	return
}

func Min(s []rune) rune {
	min := rune(-1)
	for _, r := range s {
		if min == -1 {
			min = r
			continue
		}

		if r < min {
			min = r
		}
	}

	return min
}

func IndexRune(s []rune, r rune) int {
	for i, c := range s {
		if c == r {
			return i
		}
	}
	return -1
}

func IndexLastRune(s []rune, r rune) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == r {
			return i
		}
	}

	return -1
}

func Equal(a, b []rune) bool {
	if len(a) == len(b) {
		for i := 0; i < len(a); i++ {
			if a[i] != b[i] {
				return false
			}
		}

		return true
	}

	return false
}

func HasPrefix(s, prefix []rune) bool {
	return len(s) >= len(prefix) && Equal(s[0:len(prefix)], prefix)
}

func HasSuffix(s, suffix []rune) bool {
	return len(s) >= len(suffix) && Equal(s[len(s)-len(suffix):], suffix)
}
