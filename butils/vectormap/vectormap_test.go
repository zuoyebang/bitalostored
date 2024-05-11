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
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestVectorGet(t *testing.T) {
	values := genBytesData(100, 10000)
	m := NewVectorMap(10000, WithBuckets(16))
	for i := 0; i < 10000; i++ {
		key := []byte(strconv.Itoa(i))
		value := values[i]
		m.RePut(key, value)
	}
	for i := 0; i < 16; i++ {
		go func() {
			for j := 0; ; j++ {
				if j == 10000 {
					j = 0
				}
				key := []byte(strconv.Itoa(j))
				value, closer, ok := m.Get(key)
				assert.Equal(t, true, ok)
				if !bytes.Equal(values[j], value) {
					fmt.Printf("ex: %s \nac: %s\n", string(values[j]), string(value))
				}
				if closer != nil {
					closer()
				}
			}
		}()
	}

	time.Sleep(time.Second * 10)
}

func TestVectorMapPut(t *testing.T) {
	oldValue := []byte("old")
	newValue := []byte("new")
	key := []byte("key")

	checkValue := func(getOk bool, expectOk bool, getVal, expectVal []byte) {
		assert.Equal(t, expectOk, getOk)
		assert.Equal(t, expectVal, getVal)
	}

	m := NewVectorMap(100, WithBuckets(1024))
	if ok := m.RePut(key, oldValue); !ok {
		t.Fatal("reput error")
	}
	v, closer, ok := m.Get(key)
	checkValue(ok, true, v, oldValue)
	if closer != nil {
		closer()
	}

	if ok := m.Put(key, newValue); !ok {
		t.Fatal("put error")
	}
	v, closer, ok = m.Get(key)
	checkValue(ok, true, v, newValue)
	if closer != nil {
		closer()
	}
}

func TestVectorMapPutMulti(t *testing.T) {
	values := genBytesData(256, 2)
	oldValue := values[0]
	newValue := values[1]
	key := []byte("key")

	checkValue := func(getOk bool, expectOk bool, getVal, expectVal []byte) {
		assert.Equal(t, expectOk, getOk)
		assert.Equal(t, expectVal, getVal)
	}

	m := NewVectorMap(100, WithBuckets(1024))
	if ok := m.RePut(key, oldValue); !ok {
		t.Fatal("reput error")
	}
	v, closer, ok := m.Get(key)
	checkValue(ok, true, v, oldValue)
	if closer != nil {
		closer()
	}

	if ok := m.PutMultiValue(key, 256, newValue[:128], newValue[128:]); !ok {
		t.Fatal("put error")
	}
	v, closer, ok = m.Get(key)
	checkValue(ok, true, v, newValue)
	if closer != nil {
		closer()
	}
}

func TestVectorMap_Base(t *testing.T) {
	keys := genStringData(16, 100)

	// insert
	m := NewVectorMap(2, WithDebug(), WithBuckets(1), WithEliminate(1*GB, 0, 1*time.Second))
	m.RePut([]byte(keys[0]), []byte(keys[1]))
	v, closer, ok := m.Get([]byte(keys[0]))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte(keys[1]), v)
	if closer != nil {
		closer()
	}

	lv := make([]byte, 256)
	m.RePut([]byte(keys[2]), lv)
	v, closer, ok = m.Get([]byte(keys[2]))
	assert.Equal(t, true, ok)
	assert.Equal(t, lv, v)
	if closer != nil {
		closer()
	}

	m.RePut([]byte(keys[3]), lv)
	v, closer, ok = m.Get([]byte(keys[3]))
	assert.Equal(t, true, ok)
	assert.Equal(t, lv, v)
	if closer != nil {
		closer()
	}

	for i := 0; i < 100; i += 2 {
		if ok := m.RePut([]byte(keys[i]), []byte(keys[i+1])); ok {
			v, closer, ok = m.Get([]byte(keys[i]))
			assert.Equal(t, true, ok)
			assert.Equal(t, []byte(keys[i+1]), v, "key: %s, i: %d", keys[i], i)
			if closer != nil {
				closer()
			}
		}
	}

	var resident uint32 = 0
	var memUsed uint32 = 0
	for i, _ := range m.shards[0].groups {
		for _, kIdx := range m.shards[0].groups[i] {
			k, v := m.shards[0].kvHolder.getKV(kIdx)
			if len(k) > 0 {
				resident++
				memUsed += uint32(len(v))
			}
		}
	}
	assert.Equal(t, m.shards[0].resident-m.shards[0].dead, resident, "%d : %d", m.shards[0].resident-m.shards[0].dead, resident)
	assert.Equal(t, m.shards[0].resident-m.shards[0].dead, m.shards[0].kvHolder.items)
	assert.Equal(t, memUsed, m.shards[0].kvHolder.valUsed)
	assert.Equal(t, m.Count(), int(m.Items()))

	sliceKey := []byte("slice")
	m.RePut(sliceKey, []byte("slice"))
	m.PutMultiValue(sliceKey, 8, []byte("new"), []byte("slice"))
	slice, closer, ok := m.Get(sliceKey)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("newslice"), slice)
	if closer != nil {
		closer()
	}

	m.Delete(sliceKey)
	_, closer, ok = m.Get(sliceKey)
	assert.Equal(t, false, ok)
	assert.Equal(t, m.Count(), int(m.Items()))
	if closer != nil {
		closer()
	}
	fmt.Printf(">>> %d, %d\n", m.Count(), int(m.Items()))

	m.Clear()
}

func TestVectorMap_GC_Release(t *testing.T) {
	m := NewVectorMap(4, WithDebug(), WithBuckets(1), WithEliminate(3*KB, 0, 100*time.Millisecond))
	{
		m.RePut([]byte("a"), []byte("b"))
		m.RePut([]byte("c"), make([]byte, 1024))

		_, closer, _ := m.Get([]byte("c"))
		assert.Equal(t, int32(2), m.shards[0].kvHolder.buffer.ref.refs())
		m.Delete([]byte("c"))
		m.shards[0].gcCopy()
		assert.Equal(t, int32(1), m.shards[0].kvHolder.buffer.ref.refs())
		if closer != nil {
			closer()
		}
	}

	m.Clear()
}

func TestVectorMap_GC(t *testing.T) {
	m := NewVectorMap(4, WithDebug(), WithBuckets(1), WithEliminate(3*KB, 0, 100*time.Millisecond))
	{
		m.RePut([]byte("a"), []byte("b"))
		m.RePut([]byte("c"), []byte("d"))
		m.Delete([]byte("c"))
		m.shards[0].gcCopy()
		assert.Equal(t, float32(32+20+4)/(3*1024), m.shards[0].itemsMemUsage())
		assert.Equal(t, float32(32+20+4+20+4)/(3*1024), m.shards[0].memUsage())
	}

	{
		m.RePut([]byte("c"), make([]byte, 1024))
		assert.Equal(t, float32(32+20+4+20+4+20+1024)/(3*1024), m.shards[0].memUsage())
		m.Delete([]byte("c"))
		m.shards[0].gcCopy()
		assert.Equal(t, float32(32+20+4)/(3*1024), m.shards[0].memUsage())
	}

	m.Clear()
}

func TestVectorMap_EliminateAndGC(t *testing.T) {
	m := NewVectorMap(4, WithDebug(), WithBuckets(1), WithEliminate(3*KB, 0, 100*time.Millisecond))

	{
		m.shards[0].eliminate()
		m.shards[0].gcCopy()
	}
	m.Get([]byte("b"))
	m.Get([]byte("c"))
	vlen := 992

	m.RePut([]byte("a"), make([]byte, vlen))
	m.RePut([]byte("b"), make([]byte, vlen))
	m.shards[0].eliminate()
	assert.Equal(t, float32(32+20+vlen+20+vlen)/(3*1024), m.shards[0].itemsMemUsage())
	assert.Equal(t, float32(32+20+vlen+20+vlen)/(3*1024), m.shards[0].memUsage())

	ok := m.RePut([]byte("c"), make([]byte, vlen))
	assert.Equal(t, true, ok)
	assert.Equal(t, float32(32+20+vlen+20+vlen+20+vlen)/(3*1024), m.shards[0].itemsMemUsage())
	assert.Equal(t, float32(32+20+vlen+20+vlen+20+vlen)/(3*1024), m.shards[0].memUsage())

	m.Get([]byte("a"))
	m.Get([]byte("c"))

	m.shards[0].eliminate()
	assert.Equal(t, float32(32+20+vlen+20+vlen)/(3*1024), m.shards[0].itemsMemUsage())
	assert.Equal(t, float32(32+20+vlen+20+vlen+20+vlen)/(3*1024), m.shards[0].memUsage())
	{
		_, closer, ok := m.Get([]byte("b"))
		assert.Equal(t, false, ok)
		assert.Equal(t, uint32(1), m.shards[0].dead)
		if closer != nil {
			closer()
		}
	}

	m.shards[0].gcCopy()
	assert.Equal(t, float32(32+20+vlen+20+vlen)/(3*1024), m.shards[0].itemsMemUsage())
	assert.Equal(t, float32(32+20+vlen+20+vlen)/(3*1024), m.shards[0].memUsage())

	m.Clear()
}

func TestVectorMap_WithOption(t *testing.T) {
	count := 100000
	keys := genStringData(16, 2*count)

	delKeys := make(map[string][]byte, count)
	failKeys := make(map[string][]byte, count)

	m := NewVectorMap(100000, WithBuckets(1024), WithEliminate(1*GB, 1, 1*time.Second))
	m.RePut([]byte(keys[0]), []byte(keys[1]))
	v, closer, ok := m.Get([]byte(keys[0]))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte(keys[1]), v)
	if closer != nil {
		closer()
	}

	m.RePut([]byte(keys[0]), []byte(keys[0]))
	v, closer, ok = m.Get([]byte(keys[0]))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte(keys[0]), v)
	if closer != nil {
		closer()
	}

	for i := 0; i < 2*count; i += 2 {
		if ok := m.RePut([]byte(keys[i]), []byte(keys[i+1])); !ok {
			failKeys[keys[i]] = []byte(keys[i+1])
		}
	}

	time.Sleep(5 * time.Second)

	var succ string
	for i := 0; i < 2*count; i += 2 {
		if _, del := delKeys[keys[i]]; !del {
			if _, fail := failKeys[keys[i]]; !fail {
				v, closer, ok := m.Get([]byte(keys[i]))
				assert.Equal(t, true, ok)
				assert.Equal(t, []byte(keys[i+1]), v)
				if closer != nil {
					closer()
				}
				if len(succ) == 0 {
					succ = keys[i]
				}
			}
		}
	}

	{
		v := []byte("1234567890")
		res := m.Put([]byte(succ), v)
		assert.Equal(t, true, res)

		vRes, closer, ok := m.Get([]byte(succ))
		assert.Equal(t, true, ok)
		assert.Equal(t, vRes, v)
		if closer != nil {
			closer()
		}
	}

	var resident uint32 = 0
	for i, _ := range m.shards[0].groups {
		for _, kIdx := range m.shards[0].groups[i] {
			k := m.shards[0].kvHolder.getKey(kIdx)
			if len(k) > 0 {
				resident++
			}
		}
	}
	assert.Equal(t, m.shards[0].resident-m.shards[0].dead, resident, "%d : %d", m.shards[0].resident-m.shards[0].dead, resident)

	var actualitems, expected uint32
	for i, _ := range m.shards {
		for j, _ := range m.shards[i].groups {
			for _, kIdx := range m.shards[i].groups[j] {
				k := m.shards[i].kvHolder.getKey(kIdx)
				if len(k) > 0 {
					actualitems++
				}
			}
		}
		expected += m.shards[i].resident - m.shards[i].dead
	}
	assert.Equal(t, expected, actualitems, "%d : %d", expected, actualitems)

	i := 0
	for {
		if m.RePut([]byte(keys[i]), nil) {
			res, closer, ok := m.Get([]byte(keys[i]))
			assert.Equal(t, true, ok)
			assert.Equal(t, []byte{}, res)
			if closer != nil {
				closer()
			}
			break
		}
		i += 2
	}

	m.Clear()
}

func TestParallelLongValueRW(t *testing.T) {
	m := NewVectorMap(10, WithBuckets(1), WithEliminate(1*GB, 1, 1*time.Second))
	key := []byte("1234567890")
	values := genBytesData(256, 2)
	var c = make(chan struct{}, 1)
	var oldV, newV = 0, 1
	var missHis int
	for i := 0; i < 100; i++ {
		go func() {
			var query, miss int
			for {
				select {
				case <-c:
					return
				default:
					query++
					if v, _, ok := m.Get(key); ok {
						assert.Equal(t, true, bytes.Equal(v, values[0]) || bytes.Equal(v, values[1]), " 0: %s \n 1: %s \n v: %s", string(values[0]), string(values[1]), string(v))
					} else {
						miss++
					}
				}
				if miss > missHis && miss%200 == 0 {
					missHis = miss
					fmt.Printf("query: %d, miss: %d\n", query, miss)
				}
			}
		}()
	}

	go func() {
		for {
			select {
			case <-c:
				return
			default:
				m.Put(key, values[newV])
				oldV, newV = newV, oldV
				time.Sleep(time.Millisecond * 20)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-c:
				return
			default:
				m.PutMultiValue(key, len(values[newV]), values[newV][:len(values[newV])/2], values[newV][len(values[newV])/2:])
				oldV, newV = newV, oldV
				time.Sleep(time.Millisecond * 20)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-c:
				return
			default:
				m.RePut(key, values[newV])
				oldV, newV = newV, oldV
				time.Sleep(time.Millisecond * 20)
			}
		}
	}()

	time.Sleep(1 * time.Minute)
	close(c)
	time.Sleep(time.Second)
}

func TestParallelShortValueRW(t *testing.T) {
	m := NewVectorMap(10, WithBuckets(1), WithEliminate(10*KB, 1, 1*time.Second))
	key := []byte("1234567890")
	values := genBytesData(100, 2)
	var c = make(chan struct{}, 1)
	var oldV, newV = 0, 1
	for i := 0; i < 100; i++ {
		go func() {
			for {
				select {
				case <-c:
					return
				default:
					v, closer, ok := m.Get(key)
					if ok {
						assert.Equal(t, true, bytes.Equal(v, values[0]) || bytes.Equal(v, values[1]), " 0: %s \n 1: %s \n v: %s", string(values[0]), string(values[1]), string(v))
					}
					if closer != nil {
						closer()
					}
				}
			}
		}()
	}

	go func() {
		for {
			select {
			case <-c:
				return
			default:
				m.Put(key, values[newV])
				oldV, newV = newV, oldV
			}
		}
	}()

	go func() {
		for {
			select {
			case <-c:
				return
			default:
				m.PutMultiValue(key, len(values[newV]), values[newV][:len(values[newV])/2], values[newV][len(values[newV])/2:])
				oldV, newV = newV, oldV
			}
		}
	}()

	go func() {
		for {
			select {
			case <-c:
				return
			default:
				m.RePut(key, values[newV])
				oldV, newV = newV, oldV
			}
		}
	}()

	time.Sleep(100 * time.Second)
	close(c)
	time.Sleep(time.Second)
}

func TestGCTime(t *testing.T) {
	vs := genBytesData(128, 1)
	m := NewVectorMap(4, WithDebug(), WithBuckets(1), WithEliminate(64*MB, 0, 100*time.Millisecond))
	for i := 0; i < 460000; i++ {
		m.RePut([]byte(strconv.Itoa(i)), vs[0])
	}
	t.Logf("MemUse: %d", m.shards[0].itemsUsedMem())
	t.Logf("memUsage: %.3f", m.shards[0].memUsage())
	t.Logf("Items: %d", m.shards[0].items())
	for i := 0; i < 460000; i += 9 {
		m.Delete([]byte(strconv.Itoa(i)))
	}
	t.Logf("MemUse: %d", m.shards[0].itemsUsedMem())
	t.Logf("memUsage: %.3f", m.shards[0].memUsage())
	start := time.Now()
	m.shards[0].gcCopy()
	t.Logf("gcCopy time: %s", time.Since(start))

	t.Logf("MemUse: %d", m.shards[0].itemsUsedMem())
	t.Logf("memUsage: %.3f", m.shards[0].memUsage())
	m.Clear()
}

func genBytesData(size, count int) (keys [][]byte) {
	src := rand.New(rand.NewSource(int64(size * count)))
	letters := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	r := make([]byte, size*count)
	for i := range r {
		r[i] = letters[src.Intn(len(letters))]
	}
	keys = make([][]byte, count)
	for i := range keys {
		keys[i] = r[:size]
		r = r[size:]
	}
	return
}

func genStringData(size, count int) (keys []string) {
	src := rand.New(rand.NewSource(int64(size * count)))
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	r := make([]rune, size*count)
	for i := range r {
		r[i] = letters[src.Intn(len(letters))]
	}
	keys = make([]string, count)
	for i := range keys {
		keys[i] = string(r[:size])
		r = r[size:]
	}
	return
}
