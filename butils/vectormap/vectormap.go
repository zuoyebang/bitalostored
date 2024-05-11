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
	"math"
	"sync"
	"time"
	"unsafe"

	"github.com/zuoyebang/bitalostored/butils/md5hash"
	"github.com/zuoyebang/bitalostored/butils/vectormap/simd"
)

const (
	maxLoadFactor float32 = float32(maxAvgGroupLoad) / float32(groupSize)
	MaxUint64     uint64  = 1<<64 - 1
	MaxUint32     uint32  = 1<<32 - 1
	maxCount      uint8   = 200
	maxBuckets    int     = 4096
	minBuckets    int     = 1024
	gcSleep               = 500 * time.Millisecond
	overShortSize         = 1 << 7
	overLongSize          = 1 << 15
)

type Byte uint64

const (
	B  Byte = 1
	KB      = 1024 * B
	MB      = 1024 * KB
	GB      = 1024 * MB
	TB      = 1024 * GB
	PB      = 1024 * TB
)

type ILogger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type Option func(vm *VectorMap)

func WithDebug() Option {
	return func(vm *VectorMap) {
		vm.debug = true
	}
}

func WithLogger(logger ILogger) Option {
	return func(vm *VectorMap) {
		vm.logger = logger
	}
}

func WithBuckets(buckets int) Option {
	return func(vm *VectorMap) {
		vm.buckets = buckets
	}
}

func WithEliminate(maxMem Byte, goroutines int, duration time.Duration) Option {
	return func(vm *VectorMap) {
		vm.maxMem = maxMem

		if vm.debug {
			if goroutines == 0 {
				return
			}
		}
		if goroutines <= 0 {
			goroutines = 1
		}
		if !vm.debug && duration < 8*time.Second {
			duration = 8 * time.Second
		}

		vm.eliminateHandler = &eliminateHandler{
			goroutines: goroutines,
			duration:   duration,
		}
	}
}

type VectorMap struct {
	buckets          int
	shards           []*Map
	globalMask       uint64
	maxMem           Byte
	eliminateHandler *eliminateHandler
	logger           ILogger
	debug            bool
}

func NewVectorMap(sz uint32, ops ...Option) (vm *VectorMap) {
	vm = &VectorMap{}
	for _, op := range ops {
		op(vm)
	}

	if !vm.debug {
		if vm.maxMem < 2*GB {
			vm.maxMem = 2 * GB
		} else if vm.maxMem > 128*GB {
			vm.maxMem = 128 * GB
		}

		if vm.buckets > maxBuckets {
			vm.buckets = maxBuckets
		} else if vm.buckets < minBuckets {
			vm.buckets = minBuckets
		}
	}

	power := math.Ceil(math.Log2(float64(vm.buckets)))
	vm.buckets = int(math.Pow(2, power))
	globalMask := MaxUint64 >> (64 - uint32(power))
	c := uint32(math.Ceil(float64(sz) / float64(vm.buckets)))

	vm.shards = make([]*Map, vm.buckets)
	vm.globalMask = globalMask

	for i := range vm.shards {
		vm.shards[i] = newInnerMap(vm, c)
	}

	if vm.eliminateHandler != nil {
		vm.eliminateHandler.Handle(vm)
	}
	return vm
}

//go:inline
func (vm *VectorMap) slotAt(hi uint64) *Map {
	return vm.shards[hi%uint64(vm.buckets)]
}

func (vm *VectorMap) Put(k []byte, v []byte) bool {
	h, hi, lo := md5hash.MD5Sum(k)
	return vm.slotAt(hi).Put(lo, h, v)
}

func (vm *VectorMap) PutMultiValue(k []byte, vlen int, vals ...[]byte) bool {
	h, hi, lo := md5hash.MD5Sum(k)
	return vm.slotAt(hi).PutMultiValue(lo, h, uint32(vlen), vals)
}

func (vm *VectorMap) RePut(k []byte, v []byte) bool {
	h, hi, lo := md5hash.MD5Sum(k)
	return vm.slotAt(hi).RePut(lo, h, v)
}

func (vm *VectorMap) Get(k []byte) (v []byte, closer func(), ok bool) {
	h, hi, lo := md5hash.MD5Sum(k)
	return vm.slotAt(hi).Get(lo, h)
}

func (vm *VectorMap) Delete(k []byte) {
	h, hi, lo := md5hash.MD5Sum(k)
	vm.slotAt(hi).Delete(lo, h)
}

func (vm *VectorMap) Has(k []byte) (ok bool) {
	h, hi, lo := md5hash.MD5Sum(k)
	return vm.slotAt(hi).Has(lo, h)
}

func (vm *VectorMap) Clear() {
	for _, m := range vm.shards {
		m.Clear()
	}
}

func (vm *VectorMap) Count() int {
	var sum int
	for _, m := range vm.shards {
		sum += m.Count()
	}
	return sum
}

func (vm *VectorMap) Items() uint32 {
	var sum uint32
	for _, m := range vm.shards {
		sum += m.items()
	}
	return sum
}

func (vm *VectorMap) Capacity() int {
	var sum int
	for _, m := range vm.shards {
		sum += m.Capacity()
	}
	return sum
}

func (vm *VectorMap) QueryCount() (count uint64) {
	for _, m := range vm.shards {
		count += m.queryCnt
	}
	return
}

func (vm *VectorMap) MissCount() (count uint64) {
	for _, m := range vm.shards {
		count += m.missCnt
	}
	return
}

func (vm *VectorMap) MaxMem() Byte {
	return vm.maxMem
}

func (vm *VectorMap) UsedMem() (usedMem Byte) {
	for _, m := range vm.shards {
		usedMem += (Byte)(m.kvHolder.tail)
	}
	return
}

func (vm *VectorMap) EffectiveMem() (usedMem Byte) {
	for _, m := range vm.shards {
		usedMem += m.itemsUsedMem()
	}
	return
}

type Map struct {
	owner      *VectorMap
	kvHolder   *kvHolder
	ctrl       []metadata
	counters   []counter
	groups     []group
	resident   uint32
	dead       uint32
	limit      uint32
	rehashLock sync.RWMutex
	putLock    sync.Mutex

	queryCnt uint64
	missCnt  uint64

	rehashing bool
}

type metadata [groupSize]int8
type counter [groupSize]uint8
type group [groupSize]kIdx

const (
	h1Mask    uint64 = 0xffff_ffff_ffff_ff80
	h2Mask    uint64 = 0x0000_0000_0000_007f
	empty     int8   = -128 // 0b1000_0000
	tombstone int8   = -2   // 0b1111_1110
)

type h1 uint64

type h2 int8

func newInnerMap(owner *VectorMap, sz uint32) (m *Map) {
	groups := numGroups(sz)
	m = &Map{
		owner:    owner,
		ctrl:     make([]metadata, groups),
		counters: make([]counter, groups),
		groups:   make([]group, groups),
		limit:    groups * maxAvgGroupLoad,
	}
	memMax := owner.maxMem / Byte(owner.buckets)
	if memMax > 64*MB || memMax <= 0 {
		memMax = 64 * MB
	}
	for i := range m.ctrl {
		m.ctrl[i] = newEmptyMetadata()
	}
	m.kvHolder = newKVHolder(memMax)
	return
}

//go:inline
func (m *Map) memUsed() (memused Byte) {
	memused = Byte(m.kvHolder.tail)
	return
}

//go:inline
func (m *Map) memMax() (memCap Byte) {
	memCap = Byte(m.kvHolder.cap)
	return
}

func (m *Map) itemsUsedMem() (itemsUsed Byte) {
	itemsUsed = Byte(m.kvHolder.valUsed + m.kvHolder.items*20 + 4)
	return
}

func (m *Map) items() (items uint32) {
	items = m.kvHolder.items
	return
}

//go:inline
func (m *Map) memUsage() float32 {
	return m.kvHolder.memUsage()
}

//go:inline
func (m *Map) itemsMemUsage() float32 {
	return m.kvHolder.itemsMemUsage()
}

//go:inline
func (m *Map) memUseRate() float32 {
	return m.kvHolder.memUseRate()
}

func (m *Map) Has(l uint64, key []byte) (ok bool) {
	m.queryCnt++
	m.rehashLock.RLock()
	hi, lo := splitHash(l)
	g := probeStart(hi, len(m.groups))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			m.kvHolder.mutex.RLock()
			k := m.kvHolder.getKey(m.groups[g][s])
			m.kvHolder.mutex.RUnlock()
			if bytes.Equal(key, k) {
				m.add(g, s)
				ok = true
				m.rehashLock.RUnlock()
				return
			}
		}

		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			ok = false
			m.rehashLock.RUnlock()
			m.missCnt++
			return
		}
		g += 1
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

//go:inline
func (m *Map) add(g, s uint32) {
	if m.counters[g][s] < maxCount {
		m.counters[g][s]++
	}
}

func (m *Map) Get(l uint64, key []byte) (value []byte, closer func(), ok bool) {
	m.queryCnt++
	m.rehashLock.RLock()
	hi, lo := splitHash(l)
	g := probeStart(hi, len(m.groups))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)

			m.kvHolder.mutex.RLock()
			if m.groups[g][s] == 0 {
				m.kvHolder.mutex.RUnlock()
				continue
			}
			kOffset := m.groups[g][s].offset() * 4
			k := m.kvHolder.data[kOffset : kOffset+16]
			if bytes.Equal(key, k) {
				ok = true
				kEnd := m.groups[g][s].offset()*4 + 16
				vHeader := LoadUint32(unsafe.Pointer(&m.kvHolder.data[kEnd]))
				vType := m.groups[g][s].valType()
				if vType == 0 {
					vOffset := (vHeader & IdxOffsetMask) * 4
					vSize := vHeader & IdxSmallSizeMask >> 24
					value, closer = VMBytePools.GetBytePool(int(vSize))
					copy(value, m.kvHolder.data[vOffset:vOffset+vSize])
					m.kvHolder.mutex.RUnlock()
					value = value[:vSize]
				} else {
					vOffset := (vHeader & IdxOffsetMask) * 4
					vBig := m.groups[g][s].capOrBigSize()
					vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
					value, closer = m.kvHolder.getValue(vOffset, vSize)
					m.kvHolder.mutex.RUnlock()
				}

				m.add(g, s)
				m.rehashLock.RUnlock()
				return
			} else {
				m.kvHolder.mutex.RUnlock()
			}
		}
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			ok = false
			m.rehashLock.RUnlock()
			m.missCnt++
			return
		}
		g += 1
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

func (m *Map) Put(l uint64, key []byte, value []byte) bool {
	m.putLock.Lock()
	hi, lo := splitHash(l)
	g := probeStart(hi, len(m.groups))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			k := m.kvHolder.getKey(m.groups[g][s])
			if bytes.Equal(key, k) {
				kOffset := m.groups[g][s].offset() * 4
				kEnd := kOffset + 16
				vHeader := LoadUint32(unsafe.Pointer(&m.kvHolder.data[kEnd]))
				vType := m.groups[g][s].valType()
				lv := uint32(len(value))
				if lv >= overLongSize {
					m.ctrl[g][s] = tombstone
					m.dead++
					m.counters[g][s] = 0
					m.kvHolder.items--
					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {
						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						m.kvHolder.valUsed -= Cap4Size(vSize)
					}

					m.putLock.Unlock()
					return false
				} else if lv >= overShortSize {
					vCap := Cap4Size(lv)
					ntail := m.kvHolder.tail + vCap
					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {
						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						m.kvHolder.valUsed -= Cap4Size(vSize)
					}
					if ntail > m.kvHolder.cap {
						m.ctrl[g][s] = tombstone
						m.dead++
						m.counters[g][s] = 0
						m.kvHolder.items--
						m.putLock.Unlock()
						return false
					}
					vBig := lv & 0x7f00 >> 8
					vSmall := uint32(lv) & 0xff

					copy(m.kvHolder.data[m.kvHolder.tail:], value)

					m.kvHolder.mutex.Lock()
					m.groups[g][s] = kIdx(kOffset/4 + vBig<<24 + 1<<31)
					StoreUint32(unsafe.Pointer(&(m.kvHolder.data[kEnd])), m.kvHolder.tail/4+vSmall<<24)
					m.kvHolder.mutex.Unlock()

					m.kvHolder.tail = ntail
					m.kvHolder.valUsed += vCap
				} else if vType == 0 && lv <= m.groups[g][s].capOrBigSize()*4 && lv < overShortSize {
					vOffset := vHeader & IdxOffsetMask

					m.kvHolder.mutex.Lock()
					StoreUint32(unsafe.Pointer(&(m.kvHolder.data[kEnd])), vOffset+lv<<24)
					copy(m.kvHolder.data[vOffset*4:], value)
					m.kvHolder.mutex.Unlock()
				} else {
					vCap := Cap4Size(lv)
					ntail := m.kvHolder.tail + vCap
					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {
						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						m.kvHolder.valUsed -= Cap4Size(vSize)
					}
					if ntail > m.kvHolder.cap {
						m.ctrl[g][s] = tombstone
						m.dead++
						m.counters[g][s] = 0
						m.kvHolder.items--
						m.groups[g][s] = kIdx(0)
						m.putLock.Unlock()
						return false
					}

					copy(m.kvHolder.data[m.kvHolder.tail:], value)
					m.kvHolder.mutex.Lock()
					m.groups[g][s] = kIdx(kOffset/4 + vCap/4<<24)
					StoreUint32(unsafe.Pointer(&(m.kvHolder.data[kEnd])), m.kvHolder.tail/4+(lv<<24))
					m.kvHolder.mutex.Unlock()

					m.kvHolder.tail = ntail
					m.kvHolder.valUsed += vCap
				}
				m.putLock.Unlock()
				return true
			}
		}

		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			m.putLock.Unlock()
			return false
		}
		g += 1
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

func (m *Map) PutMultiValue(l uint64, key []byte, vlen uint32, vals [][]byte) bool {
	m.putLock.Lock()
	hi, lo := splitHash(l)
	g := probeStart(hi, len(m.groups))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			k := m.kvHolder.getKey(m.groups[g][s])
			if bytes.Equal(key, k) {
				kOffset := m.groups[g][s].offset() * 4
				kEnd := kOffset + 16
				vHeader := LoadUint32(unsafe.Pointer(&m.kvHolder.data[kEnd]))
				vType := m.groups[g][s].valType()
				if vlen >= overLongSize {
					m.ctrl[g][s] = tombstone
					m.dead++
					m.counters[g][s] = 0
					m.kvHolder.items--
					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {
						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						m.kvHolder.valUsed -= Cap4Size(vSize)
					}

					m.putLock.Unlock()
					return false
				} else if vlen >= overShortSize {
					vCap := Cap4Size(vlen)
					ntail := m.kvHolder.tail + vCap
					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {
						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						m.kvHolder.valUsed -= Cap4Size(vSize)
					}
					if ntail > m.kvHolder.cap {
						m.ctrl[g][s] = tombstone
						m.dead++
						m.counters[g][s] = 0
						m.kvHolder.items--
						m.putLock.Unlock()
						return false
					}
					vBig := vlen & 0x7f00 >> 8
					vSmall := uint32(vlen) & 0xff

					vOffset := m.kvHolder.tail
					for _, v := range vals {
						copy(m.kvHolder.data[m.kvHolder.tail:], v)
						m.kvHolder.tail += uint32(len(v))
					}
					m.kvHolder.mutex.Lock()
					m.groups[g][s] = kIdx(kOffset/4 + vBig<<24 + 1<<31)
					StoreUint32(unsafe.Pointer(&(m.kvHolder.data[kEnd])), vOffset/4+vSmall<<24)
					m.kvHolder.mutex.Unlock()

					m.kvHolder.tail = ntail
					m.kvHolder.valUsed += vCap
				} else if vType == 0 && vlen <= m.groups[g][s].capOrBigSize()*4 && vlen < overShortSize {
					vOffset := vHeader & IdxOffsetMask

					m.kvHolder.mutex.Lock()
					StoreUint32(unsafe.Pointer(&(m.kvHolder.data[kEnd])), vOffset+vlen<<24)
					idx := vOffset * 4
					for _, v := range vals {
						copy(m.kvHolder.data[idx:], v)
						idx += uint32(len(v))
					}
					m.kvHolder.mutex.Unlock()
				} else {
					vCap := Cap4Size(vlen)
					ntail := m.kvHolder.tail + vCap
					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {
						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						m.kvHolder.valUsed -= Cap4Size(vSize)
					}
					if ntail > m.kvHolder.cap {
						m.ctrl[g][s] = tombstone
						m.dead++
						m.counters[g][s] = 0
						m.kvHolder.items--
						m.groups[g][s] = kIdx(0)
						m.putLock.Unlock()
						return false
					}

					vOffset := m.kvHolder.tail
					for _, v := range vals {
						copy(m.kvHolder.data[m.kvHolder.tail:], v)
						m.kvHolder.tail += uint32(len(v))
					}
					m.kvHolder.mutex.Lock()
					m.groups[g][s] = kIdx(kOffset/4 + vCap/4<<24)
					StoreUint32(unsafe.Pointer(&(m.kvHolder.data[kEnd])), vOffset/4+(vlen<<24))
					m.kvHolder.mutex.Unlock()
					m.kvHolder.tail = ntail
					m.kvHolder.valUsed += vCap
				}
				m.putLock.Unlock()
				return true
			}
		}

		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			m.putLock.Unlock()
			return false
		}
		g += 1
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

func (m *Map) RePut(l uint64, key []byte, value []byte) bool {
	if len(value) >= overLongSize {
		return false
	}

	if m.memUsed() >= m.memMax() {
		return false
	}

	if m.rehashing {
		return false
	}

	m.putLock.Lock()
	if m.resident >= m.limit {
		m.rehashing = true
		m.rehash()
		m.rehashing = false
	}

	hi, lo := splitHash(l)
	g := probeStart(hi, len(m.groups))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			k := m.kvHolder.getKey(m.groups[g][s])
			if bytes.Equal(key, k) { // update
				kOffset := m.groups[g][s].offset() * 4
				kEnd := kOffset + 16
				vHeader := LoadUint32(unsafe.Pointer(&m.kvHolder.data[kEnd]))
				vType := m.groups[g][s].valType()
				lv := uint32(len(value))
				if lv >= overLongSize {
					m.ctrl[g][s] = tombstone
					m.dead++
					m.counters[g][s] = 0

					m.kvHolder.items--
					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {
						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						m.kvHolder.valUsed -= Cap4Size(vSize)
					}

					m.putLock.Unlock()
					return false
				} else if lv >= overShortSize {
					vCap := Cap4Size(lv)
					ntail := m.kvHolder.tail + vCap
					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {
						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						m.kvHolder.valUsed -= Cap4Size(vSize)
					}
					if ntail > m.kvHolder.cap {
						m.ctrl[g][s] = tombstone
						m.dead++
						m.counters[g][s] = 0
						m.kvHolder.items--
						m.putLock.Unlock()
						return false
					}
					vBig := lv & 0x7f00 >> 8
					vSmall := uint32(lv) & 0xff

					copy(m.kvHolder.data[m.kvHolder.tail:], value)

					m.kvHolder.mutex.Lock()
					m.groups[g][s] = kIdx(kOffset/4 + vBig<<24 + 1<<31)
					StoreUint32(unsafe.Pointer(&(m.kvHolder.data[kEnd])), m.kvHolder.tail/4+vSmall<<24)
					m.kvHolder.mutex.Unlock()

					m.kvHolder.tail = ntail
					m.kvHolder.valUsed += vCap
				} else if vType == 0 && lv <= m.groups[g][s].capOrBigSize()*4 && lv < overShortSize {
					vOffset := vHeader & IdxOffsetMask

					m.kvHolder.mutex.Lock()
					StoreUint32(unsafe.Pointer(&(m.kvHolder.data[kEnd])), vOffset+lv<<24)
					copy(m.kvHolder.data[vOffset*4:], value)
					m.kvHolder.mutex.Unlock()
				} else {
					vCap := Cap4Size(lv)
					ntail := m.kvHolder.tail + vCap
					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {
						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						m.kvHolder.valUsed -= Cap4Size(vSize)
					}
					if ntail > m.kvHolder.cap {
						m.ctrl[g][s] = tombstone
						m.dead++
						m.counters[g][s] = 0
						m.kvHolder.items--
						m.groups[g][s] = kIdx(0)
						m.putLock.Unlock()
						return false
					}

					copy(m.kvHolder.data[m.kvHolder.tail:], value)
					m.kvHolder.mutex.Lock()
					m.groups[g][s] = kIdx(kOffset/4 + vCap/4<<24)
					StoreUint32(unsafe.Pointer(&(m.kvHolder.data[kEnd])), m.kvHolder.tail/4+(lv<<24))
					m.kvHolder.mutex.Unlock()

					m.kvHolder.tail = ntail
					m.kvHolder.valUsed += vCap
				}
				m.putLock.Unlock()
				return true
			}
		}

		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 { // insert
			s := nextMatch(&matches)

			lv := uint32(len(value))
			if lv >= overShortSize {
				vCap := Cap4Size(lv)
				ntail := m.kvHolder.tail + 20 + vCap
				if ntail > m.kvHolder.cap {
					m.putLock.Unlock()
					return false
				}
				vBig := lv >> 8
				vSmall := lv & 0xff

				kEnd := m.kvHolder.tail + 16
				copy(m.kvHolder.data[m.kvHolder.tail:], key)
				vOffset := kEnd + 4
				copy(m.kvHolder.data[vOffset:], value)

				m.kvHolder.mutex.Lock()
				m.groups[g][s] = kIdx(m.kvHolder.tail/4 + vBig<<24 + 1<<31)
				StoreUint32(unsafe.Pointer(&(m.kvHolder.data[kEnd])), vOffset/4+(vSmall<<24))
				m.kvHolder.mutex.Unlock()

				m.kvHolder.items++
				m.kvHolder.valUsed += vCap
				m.kvHolder.tail = ntail

				m.ctrl[g][s] = int8(lo)
				m.counters[g][s] = 1
				m.resident++

				m.putLock.Unlock()
				return true
			} else {
				vCap := Cap4Size(lv)
				ntail := m.kvHolder.tail + 20 + vCap
				if ntail > m.kvHolder.cap {
					m.putLock.Unlock()
					return false
				}
				vSmall := lv

				kEnd := m.kvHolder.tail + 16
				copy(m.kvHolder.data[m.kvHolder.tail:], key)
				vOffset := kEnd + 4
				copy(m.kvHolder.data[vOffset:], value)

				m.kvHolder.mutex.Lock()
				m.groups[g][s] = kIdx(m.kvHolder.tail/4 + vCap/4<<24)
				StoreUint32(unsafe.Pointer(&(m.kvHolder.data[kEnd])), vOffset/4+(vSmall<<24))
				m.kvHolder.mutex.Unlock()

				m.kvHolder.items++
				m.kvHolder.valUsed += vCap
				m.kvHolder.tail = ntail

				m.ctrl[g][s] = int8(lo)
				m.counters[g][s] = 1
				m.resident++

				m.putLock.Unlock()
				return true
			}
		}
		g += 1
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

func (m *Map) Delete(l uint64, key []byte) (ok bool) {
	m.putLock.Lock()
	hi, lo := splitHash(l)
	g := probeStart(hi, len(m.groups))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			k := m.kvHolder.getKey(m.groups[g][s])
			if bytes.Equal(key, k) {
				m.kvHolder.del(m.groups[g][s])
				ok = true
				if metaMatchEmpty(&m.ctrl[g]) != 0 {
					m.ctrl[g][s] = empty
					m.resident--
				} else {
					m.ctrl[g][s] = tombstone
					m.dead++
				}
				m.counters[g][s] = 0
				m.putLock.Unlock()
				return
			}
		}
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			ok = false
			m.putLock.Unlock()
			return
		}
		g += 1
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

func (m *Map) Clear() {
	for i, c := range m.ctrl {
		for j := range c {
			m.ctrl[i][j] = empty
		}
	}
	for i, c := range m.counters {
		for j := range c {
			m.counters[i][j] = 0
		}
	}
	for i, g := range m.groups {
		for j := range g {
			m.groups[i][j] = 0
		}
	}
	m.resident, m.dead = 0, 0

	m.kvHolder.cap = 0
	m.kvHolder.data = nil
}

func (m *Map) Count() int {
	return int(m.resident - m.dead)
}

func (m *Map) Capacity() int {
	return int(m.limit - m.resident)
}

//go:inline
func (m *Map) nextSize() (n uint32) {
	n = uint32(math.Ceil(float64(len(m.groups)) * 1.2))
	if m.dead >= (m.resident / 2) {
		n = uint32(len(m.groups))
	}
	return
}

func (m *Map) rehash() {
	n := m.nextSize()
	groups := make([]group, n)
	ctrl := make([]metadata, n)
	counters := make([]counter, n)
	kvholder := newKVHolder(Byte(m.kvHolder.cap))
	for i := range ctrl {
		ctrl[i] = newEmptyMetadata()
	}
	var resident uint32
	for g := range m.ctrl {
		for s := range m.ctrl[g] {
			c := m.ctrl[g][s]
			if c == empty || c == tombstone {
				continue
			}
			k, v := m.kvHolder.getKVUnlock(m.groups[g][s])

			_, l := md5hash.MD5HL(k)
			hi, lo := splitHash(l)
			gN := probeStart(hi, len(groups))
			for {
				matches := metaMatchEmpty(&ctrl[gN])
				if matches != 0 {
					sN := nextMatch(&matches)
					groups[gN][sN], _ = kvholder.gcSet(k, v)
					ctrl[gN][sN] = int8(lo)
					counters[gN][sN] = m.counters[g][s]
					resident++
					break
				}
				gN++
				if gN >= uint32(len(groups)) {
					gN = 0
				}
			}
		}
	}

	m.rehashLock.Lock()
	m.groups = groups
	m.ctrl = ctrl
	m.counters = counters
	m.kvHolder = kvholder
	m.limit = n * maxAvgGroupLoad
	m.resident, m.dead = resident, 0
	m.rehashLock.Unlock()
}

func (m *Map) loadFactor() float32 {
	slots := float32(len(m.groups) * groupSize)
	return float32(m.resident-m.dead) / slots
}

const (
	eliminateStart = 0.9
	eliminateEnd   = 0.85
	missRate       = 0.1
)

func (m *Map) eliminate() (delCount int) {
	if m.queryCnt > 0 && float32(m.missCnt)/float32(m.queryCnt) < missRate {
		return
	}

	usedRate := m.itemsMemUsage()
	if usedRate < eliminateStart {
		return
	}

	n := int(math.Ceil(float64(float32(m.kvHolder.items) * (eliminateStart - eliminateEnd) / eliminateStart)))
	if n == 0 {
		return
	}

	m.putLock.Lock()
	item, x := BuildMinTop(m.ctrl, m.counters, n)
	itemLen := len(item)

	for i := 0; i < itemLen; i++ {
		g, s := item[i].g, item[i].s
		if m.ctrl[g][s] == tombstone || m.ctrl[g][s] == empty {
			continue
		}
		m.rehashLock.Lock()
		m.kvHolder.del(m.groups[g][s])
		m.groups[g][s] = 0
		m.ctrl[g][s] = tombstone
		m.rehashLock.Unlock()
		m.dead++
		delCount++
	}

	var level [16]uint8
	for i := 0; i < 16; i++ {
		level[i] = x
	}

	ctrLen := len(m.ctrl)
	for i := 0; i < ctrLen; i++ {
		simd.MSubs128epu8(unsafe.Pointer(&(m.counters[i])), unsafe.Pointer(&level), unsafe.Pointer(&(m.counters[i])))
	}
	m.putLock.Unlock()
	return
}

const (
	gcCheckL1 = 0.3
	gcCheckL2 = 0.6
	gcCheckL3 = 0.9
)

var gcThreshold = map[float32]float32{
	gcCheckL1: 0.5,
	gcCheckL2: 0.35,
	gcCheckL3: 0.1,
}

func (m *Map) gcCopy() (deadCount int, gcMem int) {
	usedRate := m.memUseRate()
	gRate := 1.0 - usedRate
	memUsage := m.memUsage()

	if memUsage < gcCheckL1 ||
		(memUsage < gcCheckL2 && gRate < gcThreshold[gcCheckL1]) ||
		(memUsage < gcCheckL3 && gRate < gcThreshold[gcCheckL2]) ||
		gRate < gcThreshold[gcCheckL3] {
		return
	}
	if m.rehashing {
		return
	} else {
		m.rehashing = true
	}
	oldUsed := m.kvHolder.tail
	deadCount = int(m.dead)
	n := uint32(len(m.groups))
	groups := make([]group, n)
	ctrl := make([]metadata, n)
	counters := make([]counter, n)
	kvholder := newKVHolder(Byte(m.kvHolder.cap))

	m.putLock.Lock()
	for i := range ctrl {
		ctrl[i] = newEmptyMetadata()
	}

	for g := range m.ctrl {
		for s := range m.ctrl[g] {
			c := m.ctrl[g][s]
			if c == empty || c == tombstone {
				continue
			}
			k, v := m.kvHolder.getKVUnlock(m.groups[g][s])

			_, l := md5hash.MD5HL(k)
			hi, lo := splitHash(l)
			gN := probeStart(hi, len(groups))
			for {
				matches := metaMatchEmpty(&ctrl[gN])
				if matches != 0 {
					sN := nextMatch(&matches)
					groups[gN][sN], _ = kvholder.gcSet(k, v)
					ctrl[gN][sN] = int8(lo)
					counters[gN][sN] = m.counters[g][s]
					break
				}
				gN++
				if gN >= uint32(len(groups)) {
					gN = 0
				}
			}
		}
	}

	m.rehashLock.Lock()
	m.groups = groups
	m.ctrl = ctrl
	m.counters = counters
	m.kvHolder.buffer.release()
	m.kvHolder = kvholder
	m.resident, m.dead = m.resident-m.dead, 0
	m.rehashLock.Unlock()
	m.putLock.Unlock()
	m.rehashing = false
	gcMem = int(oldUsed - m.kvHolder.tail)
	return
}

type eliminateHandler struct {
	goroutines int
	duration   time.Duration
}

func (h *eliminateHandler) Handle(vm *VectorMap) {
	for i := 0; i < h.goroutines; i++ {
		go func(idx int) {
			var doEli = false
			for {
				start := time.Now()
				var eliMaps, eliItems, gcMaps, gcItems, gcMem int
				for j := idx; j < vm.buckets; j += h.goroutines {
					if doEli {
						if ec := vm.shards[j].eliminate(); ec > 0 {
							eliMaps++
							eliItems += ec
						}
					}
					if gcI, gcM := vm.shards[j].gcCopy(); gcI > 0 {
						gcMaps++
						gcItems += gcI
						gcMem += gcM
					}
					time.Sleep(gcSleep)
				}
				cost := time.Since(start)
				if vm.logger != nil {
					vm.logger.Infof("eliminate index %d cost: %v, eliMaps: %d, eliItems: %d, gcMaps: %d, gcItems: %d, gcMem: %d",
						idx, cost, eliMaps, eliItems, gcMaps, gcItems, gcMem)
				}
				doEli = !doEli
				time.Sleep(h.duration + time.Duration(randIntN(int(h.duration)/1e9)*1e9))
			}
		}(i)
	}
}

func numGroups(n uint32) (groups uint32) {
	groups = (n + maxAvgGroupLoad - 1) / maxAvgGroupLoad
	if groups == 0 {
		groups = 1
	}
	return
}

func newEmptyMetadata() (meta metadata) {
	for i := range meta {
		meta[i] = empty
	}
	return
}

func splitHash(h uint64) (h1, h2) {
	return h1((h & h1Mask) >> 7), h2(h & h2Mask)
}

func probeStart(hi h1, groups int) uint32 {
	return fastModN(uint32(hi), uint32(groups))
}

func fastModN(x, n uint32) uint32 {
	return uint32((uint64(x) * uint64(n)) >> 32)
}
