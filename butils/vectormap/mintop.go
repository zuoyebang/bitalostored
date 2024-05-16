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

type Item[V uint8 | uint16] struct {
	g     uint32
	s     uint8
	value V
}

type minTop[V uint8 | uint16] struct {
	items []*Item[V]
	len   int
	cap   int
}

func (h *minTop[V]) Len() int { return h.len }

func (h *minTop[V]) Less(i, j int) bool { return h.items[i].value > h.items[j].value }

func (h *minTop[V]) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *minTop[V]) Push(x *Item[V]) {
	if h.len == h.cap {
		h.items[0] = x
	} else {
		h.items = append(h.items, x)
		h.len++
	}
}

func Push[V uint8 | uint16](h *minTop[V], x *Item[V]) (bre bool) {
	if h.len == h.cap {
		if h.items[0].value == 0 {
			return true
		}
		if x.value >= h.items[0].value {
			return
		}
		h.Push(x)
		if !down(h, 0, h.Len()) {
			up(h, 0)
		}
	} else {
		h.len++
		h.Push(x)
		up(h, h.Len()-1)
	}
	return
}

func up[V uint8 | uint16](h *minTop[V], j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func down[V uint8 | uint16](h *minTop[V], i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}

func BuildMinTopCounter[V uint8 | uint16](ctrl []metadata, counters []counter, l int) ([]*Item[V], uint8) {
	if l == 0 {
		return nil, 0
	}
	h := &minTop[V]{cap: l}
	h.items = make([]*Item[V], l)
	for g, _ := range counters {
		left := groupSize
		for i := 0; h.len < h.cap && i < groupSize; i++ {
			if ctrl[g][i] == empty || ctrl[g][i] == tombstone {
				left--
				continue
			}
			h.items[h.len] = &Item[V]{value: V(counters[g][i]), g: uint32(g), s: uint8(i)}
			h.len++
			left--
			n := h.Len()
			for i := n/2 - 1; i >= 0; i-- {
				down(h, i, n)
			}
		}

		for s := 0; left > 0; left-- {
			s = groupSize - left
			if ctrl[g][s] == empty || ctrl[g][s] == tombstone {
				continue
			}
			Push(h, &Item[V]{value: V(counters[g][s]), g: uint32(g), s: uint8(s)})
		}
	}
	if h.len == 0 {
		return nil, 0
	}
	return h.items[:h.len], uint8(h.items[0].value)
}

func BuildMinTopSince[V uint8 | uint16](ctrl []metadata, counters []since, l int) ([]*Item[V], uint16) {
	if l == 0 {
		return nil, 0
	}
	h := &minTop[V]{cap: l}
	h.items = make([]*Item[V], l)
	for g, _ := range counters {
		left := groupSize
		for i := 0; h.len < h.cap && i < groupSize; i++ {
			if ctrl[g][i] == empty || ctrl[g][i] == tombstone {
				left--
				continue
			}
			h.items[h.len] = &Item[V]{value: V(counters[g][i]), g: uint32(g), s: uint8(i)}
			h.len++
			left--
			n := h.Len()
			for i := n/2 - 1; i >= 0; i-- {
				down(h, i, n)
			}
		}

		for s := 0; left > 0; left-- {
			s = groupSize - left
			if ctrl[g][s] == empty || ctrl[g][s] == tombstone {
				continue
			}
			Push(h, &Item[V]{value: V(counters[g][s]), g: uint32(g), s: uint8(s)})
		}
	}
	if h.len == 0 {
		return nil, 0
	}
	return h.items[:h.len], uint16(h.items[0].value)
}
