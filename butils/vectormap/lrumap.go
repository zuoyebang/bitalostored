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
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/zuoyebang/bitalostored/butils/vectormap/simd"

	"github.com/zuoyebang/bitalostored/butils/md5hash"
)

var UnitTime = 30 * time.Second

const LRUSubDuration = 24 * time.Hour

type LRUMap struct {
	owner      *VectorMap
	kvHolder   *kvHolder
	ctrl       []metadata
	sinces     []since
	groups     []group
	resident   uint32
	dead       uint32
	limit      uint32
	rehashLock sync.RWMutex
	putLock    sync.Mutex

	startTime   time.Time
	lastSubTime time.Time

	queryCnt    atomic.Uint64
	missCnt     atomic.Uint64
	minTopSince uint16
	rehashing   bool
}

func newInnerLRUMap(owner *VectorMap, sz uint32) (m *LRUMap) {
	groups := numGroups(sz)
	m = &LRUMap{
		startTime:   time.Now(),
		lastSubTime: time.Now(),
		owner:       owner,
		ctrl:        make([]metadata, groups),
		sinces:      make([]since, groups),
		groups:      make([]group, groups),
		limit:       groups * maxAvgGroupLoad,
	}
	memMax := owner.memCap / Byte(owner.buckets)
	if memMax > 64*MB || memMax <= 0 {
		memMax = 64 * MB
	}
	for i := range m.ctrl {
		m.ctrl[i] = newEmptyMetadata()
	}
	m.kvHolder = newKVHolder(memMax)
	return
}

func (m *LRUMap) kvholder() *kvHolder {
	return m.kvHolder
}

func (m *LRUMap) Groups() []group {
	return m.groups
}

func (m *LRUMap) Resident() uint32 {
	return m.resident
}

func (m *LRUMap) Dead() uint32 {
	return m.dead
}

//go:inline
func (m *LRUMap) memUsed() (memused Byte) {
	memused = Byte(m.kvHolder.tail)
	return
}

//go:inline
func (m *LRUMap) memMax() (memCap Byte) {
	memCap = Byte(m.kvHolder.cap)
	return
}

func (m *LRUMap) UsedMem() (used Byte) {
	used = Byte(m.kvHolder.tail)
	return
}

func (m *LRUMap) ItemsUsedMem() (itemsUsed Byte) {
	itemsUsed = Byte(m.kvHolder.valUsed + m.kvHolder.items*20 + 4)
	return
}

func (m *LRUMap) Items() (items uint32) {
	items = m.kvHolder.items
	return
}

//go:inline
func (m *LRUMap) memUsage() float32 {
	return m.kvHolder.memUsage()
}

//go:inline
func (m *LRUMap) itemsMemUsage() float32 {
	return m.kvHolder.itemsMemUsage()
}

//go:inline
func (m *LRUMap) garbageUsage() float32 {
	return m.kvHolder.garbageUsage()
}

//go:inline
func (m *LRUMap) memUseRate() float32 {
	return m.kvHolder.memUseRate()
}

func (m *LRUMap) Has(l uint64, key []byte) (ok bool) {
	m.queryCnt.Add(1)
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
				ok = true
				m.sinces[g][s] = uint16(time.Since(m.startTime) / UnitTime)
				m.rehashLock.RUnlock()
				return
			}
		}

		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			ok = false
			m.rehashLock.RUnlock()
			m.missCnt.Add(1)
			return
		}
		g += 1
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

func (m *LRUMap) Get(l uint64, key []byte) (value []byte, closer func(), ok bool) {
	m.queryCnt.Add(1)
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
				vHeader := LoadUint32(m.kvHolder.data[kOffset+16:])
				vType := m.groups[g][s].valType()
				if vType == 0 {
					vOffset := (vHeader & IdxOffsetMask) * 4
					vSize := vHeader & IdxSmallSizeMask >> 24
					value, closer = VMBytePools.GetBytePool(int(vSize))
					copy(value, m.kvHolder.data[vOffset:vOffset+vSize])
					m.sinces[g][s] = uint16(time.Since(m.startTime) / UnitTime)
					m.kvHolder.mutex.RUnlock()
					value = value[:vSize]
				} else {
					vOffset := (vHeader & IdxOffsetMask) * 4
					vBig := m.groups[g][s].capOrBigSize()
					vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
					if vSize == overLongSize {
						vSize = LoadUint32(m.kvHolder.data[vOffset:])
						value, closer = m.kvHolder.getValue(vOffset+4, vSize)
					} else {
						value, closer = m.kvHolder.getValue(vOffset, vSize)
					}

					m.sinces[g][s] = uint16(time.Since(m.startTime) / UnitTime)
					m.kvHolder.mutex.RUnlock()
				}

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
			m.missCnt.Add(1)
			return
		}
		g += 1
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

func (m *LRUMap) Put(l uint64, key []byte, value []byte) bool {
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
				vHeader := LoadUint32(m.kvHolder.data[kEnd:])
				vType := m.groups[g][s].valType()
				lv := uint32(len(value))
				if lv >= limitSize {
					m.ctrl[g][s] = tombstone
					m.dead++

					m.kvHolder.items--
					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {
						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						if vSize == overLongSize {
							vOffset := (vHeader & IdxOffsetMask) * 4
							vSize = LoadUint32(m.kvHolder.data[vOffset:])
							m.kvHolder.valUsed -= Cap4Size(vSize) + 4
						} else {
							m.kvHolder.valUsed -= Cap4Size(vSize)
						}
					}

					m.putLock.Unlock()
					return false
				} else if lv >= overLongSize {
					vCap := Cap4Size(lv) + 4
					ntail := m.kvHolder.tail + 20 + vCap
					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {
						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						if vSize == overLongSize {
							vOffset := (vHeader & IdxOffsetMask) * 4
							vSize = LoadUint32(m.kvHolder.data[vOffset:])
							m.kvHolder.valUsed -= Cap4Size(vSize) + 4
						} else {
							m.kvHolder.valUsed -= Cap4Size(vSize)
						}
					}
					if ntail > m.kvHolder.cap {
						m.ctrl[g][s] = tombstone
						m.dead++
						m.kvHolder.items--
						m.putLock.Unlock()
						return false
					}

					vOffset := m.kvHolder.tail
					StoreUint32(m.kvHolder.data[vOffset:], lv)
					copy(m.kvHolder.data[vOffset+4:], value)

					m.kvHolder.mutex.Lock()
					m.groups[g][s] = kIdx(kOffset/storeUintBytes + overLongStoreHeaderH + mapTypeHeader)
					m.sinces[g][s] = uint16(time.Since(m.startTime) / UnitTime)
					StoreUint32(m.kvHolder.data[kEnd:], vOffset/storeUintBytes+overLongStoreHeaderL)
					m.kvHolder.mutex.Unlock()

					m.kvHolder.tail = ntail
					m.kvHolder.valUsed += vCap
				} else if lv >= overShortSize {
					vCap := Cap4Size(lv)
					ntail := m.kvHolder.tail + vCap
					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {
						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						if vSize == overLongSize {
							vOffset := (vHeader & IdxOffsetMask) * 4
							vSize = LoadUint32(m.kvHolder.data[vOffset:])
							m.kvHolder.valUsed -= Cap4Size(vSize) + 4
						} else {
							m.kvHolder.valUsed -= Cap4Size(vSize)
						}
					}
					if ntail > m.kvHolder.cap {
						m.ctrl[g][s] = tombstone
						m.dead++

						m.kvHolder.items--
						m.putLock.Unlock()
						return false
					}
					vBig := lv & 0x7f00 >> 8
					vSmall := uint32(lv) & 0xff

					copy(m.kvHolder.data[m.kvHolder.tail:], value)

					m.kvHolder.mutex.Lock()
					m.groups[g][s] = kIdx(kOffset/storeUintBytes + vBig<<24 + mapTypeHeader)
					m.sinces[g][s] = uint16(time.Since(m.startTime) / UnitTime)
					StoreUint32(m.kvHolder.data[kEnd:], m.kvHolder.tail/storeUintBytes+vSmall<<24)
					m.kvHolder.mutex.Unlock()

					m.kvHolder.tail = ntail
					m.kvHolder.valUsed += vCap
				} else if vType == 0 && lv <= m.groups[g][s].capOrBigSize()*4 && lv < overShortSize {
					vOffset := vHeader & IdxOffsetMask

					m.kvHolder.mutex.Lock()
					StoreUint32(m.kvHolder.data[kEnd:], vOffset+lv<<24)
					m.sinces[g][s] = uint16(time.Since(m.startTime) / UnitTime)
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
						if vSize == overLongSize {
							vOffset := (vHeader & IdxOffsetMask) * 4
							vSize = LoadUint32(m.kvHolder.data[vOffset:])
							m.kvHolder.valUsed -= Cap4Size(vSize) + 4
						} else {
							m.kvHolder.valUsed -= Cap4Size(vSize)
						}
					}
					if ntail > m.kvHolder.cap {
						m.ctrl[g][s] = tombstone
						m.dead++

						m.kvHolder.items--
						m.groups[g][s] = kIdx(0)
						m.putLock.Unlock()
						return false
					}

					copy(m.kvHolder.data[m.kvHolder.tail:], value)
					m.kvHolder.mutex.Lock()
					m.groups[g][s] = kIdx(kOffset/storeUintBytes + vCap/storeUintBytes<<24)
					m.sinces[g][s] = uint16(time.Since(m.startTime) / UnitTime)
					StoreUint32(m.kvHolder.data[kEnd:], m.kvHolder.tail/storeUintBytes+(lv<<24))
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

func (m *LRUMap) PutMultiValue(l uint64, key []byte, vlen uint32, vals [][]byte) bool {
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
				vHeader := LoadUint32(m.kvHolder.data[kEnd:])
				vType := m.groups[g][s].valType()
				if vlen >= limitSize {
					m.ctrl[g][s] = tombstone
					m.dead++

					m.kvHolder.items--
					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {
						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						if vSize == overLongSize {
							vOffset := (vHeader & IdxOffsetMask) * 4
							vSize = LoadUint32(m.kvHolder.data[vOffset:])
							m.kvHolder.valUsed -= Cap4Size(vSize) + 4
						} else {
							m.kvHolder.valUsed -= Cap4Size(vSize)
						}
					}

					m.putLock.Unlock()
					return false
				} else if vlen >= overLongSize {
					vCap := Cap4Size(vlen) + 4
					ntail := m.kvHolder.tail + 20 + vCap
					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {

						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						if vSize == overLongSize {
							vOffset := (vHeader & IdxOffsetMask) * 4
							vSize = LoadUint32(m.kvHolder.data[vOffset:])
							m.kvHolder.valUsed -= Cap4Size(vSize) + 4
						} else {
							m.kvHolder.valUsed -= Cap4Size(vSize)
						}
					}
					if ntail > m.kvHolder.cap {
						m.ctrl[g][s] = tombstone
						m.dead++
						m.kvHolder.items--
						m.putLock.Unlock()
						return false
					}
					vOffset := m.kvHolder.tail
					StoreUint32(m.kvHolder.data[vOffset:], vlen)
					m.kvHolder.tail += 4
					for _, v := range vals {
						copy(m.kvHolder.data[m.kvHolder.tail:], v)
						m.kvHolder.tail += uint32(len(v))
					}
					m.kvHolder.mutex.Lock()
					m.groups[g][s] = kIdx(kOffset/storeUintBytes + overLongStoreHeaderH + mapTypeHeader)
					m.sinces[g][s] = uint16(time.Since(m.startTime) / UnitTime)
					StoreUint32(m.kvHolder.data[kEnd:], vOffset/storeUintBytes+overLongStoreHeaderL)
					m.kvHolder.mutex.Unlock()

					m.kvHolder.tail = ntail
					m.kvHolder.valUsed += vCap
				} else if vlen >= overShortSize {
					vCap := Cap4Size(vlen)
					ntail := m.kvHolder.tail + vCap
					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {
						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						if vSize == overLongSize {
							vOffset := (vHeader & IdxOffsetMask) * 4
							vSize = LoadUint32(m.kvHolder.data[vOffset:])
							m.kvHolder.valUsed -= Cap4Size(vSize) + 4
						} else {
							m.kvHolder.valUsed -= Cap4Size(vSize)
						}
					}
					if ntail > m.kvHolder.cap {
						m.ctrl[g][s] = tombstone
						m.dead++
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
					m.groups[g][s] = kIdx(kOffset/storeUintBytes + vBig<<24 + mapTypeHeader)
					m.sinces[g][s] = uint16(time.Since(m.startTime) / UnitTime)
					StoreUint32(m.kvHolder.data[kEnd:], vOffset/storeUintBytes+vSmall<<24)
					m.kvHolder.mutex.Unlock()

					m.kvHolder.tail = ntail
					m.kvHolder.valUsed += vCap
				} else if vType == 0 && vlen <= m.groups[g][s].capOrBigSize()*4 && vlen < overShortSize {
					vOffset := vHeader & IdxOffsetMask

					m.kvHolder.mutex.Lock()
					StoreUint32(m.kvHolder.data[kEnd:], vOffset+vlen<<24)
					idx := vOffset * 4
					for _, v := range vals {
						copy(m.kvHolder.data[idx:], v)
						idx += uint32(len(v))
					}
					m.sinces[g][s] = uint16(time.Since(m.startTime) / UnitTime)
					m.kvHolder.mutex.Unlock()
				} else {
					vCap := Cap4Size(vlen)
					ntail := m.kvHolder.tail + vCap
					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {
						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						if vSize == overLongSize {
							vOffset := (vHeader & IdxOffsetMask) * 4
							vSize = LoadUint32(m.kvHolder.data[vOffset:])
							m.kvHolder.valUsed -= Cap4Size(vSize) + 4
						} else {
							m.kvHolder.valUsed -= Cap4Size(vSize)
						}
					}
					if ntail > m.kvHolder.cap {
						m.ctrl[g][s] = tombstone
						m.dead++

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
					m.groups[g][s] = kIdx(kOffset/storeUintBytes + vCap/storeUintBytes<<24)
					m.sinces[g][s] = uint16(time.Since(m.startTime) / UnitTime)
					StoreUint32(m.kvHolder.data[kEnd:], vOffset/storeUintBytes+(vlen<<24))
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

func (m *LRUMap) RePut(l uint64, key []byte, value []byte) bool {
	if m.kvHolder.tail >= m.kvHolder.limit {
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
				vHeader := LoadUint32(m.kvHolder.data[kEnd:])
				vType := m.groups[g][s].valType()
				lv := uint32(len(value))
				if lv >= overLongSize {
					vCap := Cap4Size(lv) + 4
					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {
						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						if vSize == overLongSize {
							vOffset := (vHeader & IdxOffsetMask) * 4
							vSize = LoadUint32(m.kvHolder.data[vOffset:])
							m.kvHolder.valUsed -= Cap4Size(vSize) + 4
						} else {
							m.kvHolder.valUsed -= Cap4Size(vSize)
						}
					}
					vOffset := m.kvHolder.tail + 4
					ntail := vOffset + vCap
					if ntail > m.kvHolder.cap {
						m.ctrl[g][s] = tombstone
						m.dead++

						m.kvHolder.items--
						m.putLock.Unlock()
						return false
					}
					StoreUint32(m.kvHolder.data[m.kvHolder.tail:], lv)
					copy(m.kvHolder.data[vOffset:], value)

					m.kvHolder.mutex.Lock()
					m.groups[g][s] = kIdx(kOffset/storeUintBytes + overLongStoreHeaderH + mapTypeHeader)
					StoreUint32(m.kvHolder.data[kEnd:], m.kvHolder.tail/storeUintBytes+overLongStoreHeaderL)
					m.sinces[g][s] = uint16(time.Since(m.startTime) / UnitTime)
					m.kvHolder.mutex.Unlock()

					m.kvHolder.tail = ntail
					m.kvHolder.valUsed += vCap
				} else if lv >= overShortSize {
					vCap := Cap4Size(lv)
					ntail := m.kvHolder.tail + vCap

					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {
						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						if vSize == overLongSize {
							vOffset := (vHeader & IdxOffsetMask) * 4
							vSize = LoadUint32(m.kvHolder.data[vOffset:])
							m.kvHolder.valUsed -= Cap4Size(vSize) + 4
						} else {
							m.kvHolder.valUsed -= Cap4Size(vSize)
						}
					}

					if ntail > m.kvHolder.cap {
						m.ctrl[g][s] = tombstone
						m.dead++

						m.kvHolder.items--
						m.putLock.Unlock()
						return false
					}
					vBig := lv & 0x7f00 >> 8
					vSmall := uint32(lv) & 0xff

					copy(m.kvHolder.data[m.kvHolder.tail:], value)

					m.kvHolder.mutex.Lock()
					m.groups[g][s] = kIdx(kOffset/storeUintBytes + vBig<<24 + mapTypeHeader)
					StoreUint32(m.kvHolder.data[kEnd:], m.kvHolder.tail/storeUintBytes+vSmall<<24)
					m.sinces[g][s] = uint16(time.Since(m.startTime) / UnitTime)
					m.kvHolder.mutex.Unlock()

					m.kvHolder.tail = ntail
					m.kvHolder.valUsed += vCap
				} else if vType == 0 && lv <= m.groups[g][s].capOrBigSize()*4 && lv < overShortSize {
					vOffset := vHeader & IdxOffsetMask

					m.kvHolder.mutex.Lock()
					StoreUint32(m.kvHolder.data[kEnd:], vOffset+lv<<24)
					copy(m.kvHolder.data[vOffset*4:], value)
					m.sinces[g][s] = uint16(time.Since(m.startTime) / UnitTime)
					m.kvHolder.mutex.Unlock()
				} else {
					vCap := Cap4Size(lv)
					ntail := m.kvHolder.tail + vCap
					if vType == 0 {
						m.kvHolder.valUsed -= m.groups[g][s].capOrBigSize()
					} else {
						vBig := m.groups[g][s].capOrBigSize()
						vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
						if vSize == overLongSize {
							vOffset := (vHeader & IdxOffsetMask) * 4
							vSize = LoadUint32(m.kvHolder.data[vOffset:])
							m.kvHolder.valUsed -= Cap4Size(vSize) + 4
						} else {
							m.kvHolder.valUsed -= Cap4Size(vSize)
						}
					}
					if ntail > m.kvHolder.cap {
						m.ctrl[g][s] = tombstone
						m.dead++

						m.kvHolder.items--
						m.groups[g][s] = kIdx(0)
						m.putLock.Unlock()
						return false
					}

					copy(m.kvHolder.data[m.kvHolder.tail:], value)
					m.kvHolder.mutex.Lock()
					m.groups[g][s] = kIdx(kOffset/storeUintBytes + vCap/storeUintBytes<<24)
					m.sinces[g][s] = uint16(time.Since(m.startTime) / UnitTime)
					StoreUint32(m.kvHolder.data[kEnd:], m.kvHolder.tail/storeUintBytes+(lv<<24))
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
			s := nextMatch(&matches)

			lv := uint32(len(value))
			if lv >= overLongSize {
				vCap := Cap4Size(lv) + 4
				ntail := m.kvHolder.tail + 20 + vCap
				if ntail > m.kvHolder.cap {
					m.putLock.Unlock()
					return false
				}

				kEnd := m.kvHolder.tail + 16
				copy(m.kvHolder.data[m.kvHolder.tail:], key)
				vOffset := kEnd + 4
				StoreUint32(m.kvHolder.data[vOffset:], lv)
				copy(m.kvHolder.data[vOffset+4:], value)
				m.kvHolder.mutex.Lock()
				m.groups[g][s] = kIdx(m.kvHolder.tail/storeUintBytes + overLongStoreHeaderH + mapTypeHeader)
				StoreUint32(m.kvHolder.data[kEnd:], vOffset/storeUintBytes+(overLongStoreHeaderL))
				m.kvHolder.mutex.Unlock()

				m.kvHolder.items++
				m.kvHolder.valUsed += vCap
				m.kvHolder.tail = ntail

				m.ctrl[g][s] = int8(lo)
				m.sinces[g][s] = uint16(time.Since(m.startTime) / UnitTime)
				m.resident++

				m.putLock.Unlock()
				return true
			} else if lv >= overShortSize {
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
				m.groups[g][s] = kIdx(m.kvHolder.tail/storeUintBytes + vBig<<24 + mapTypeHeader)
				StoreUint32(m.kvHolder.data[kEnd:], vOffset/storeUintBytes+(vSmall<<24))
				m.kvHolder.mutex.Unlock()

				m.kvHolder.items++
				m.kvHolder.valUsed += vCap
				m.kvHolder.tail = ntail

				m.ctrl[g][s] = int8(lo)
				m.sinces[g][s] = uint16(time.Since(m.startTime) / UnitTime)
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
				m.groups[g][s] = kIdx(m.kvHolder.tail/storeUintBytes + vCap/storeUintBytes<<24)
				StoreUint32(m.kvHolder.data[kEnd:], vOffset/storeUintBytes+(vSmall<<24))
				m.kvHolder.mutex.Unlock()

				m.kvHolder.items++
				m.kvHolder.valUsed += vCap
				m.kvHolder.tail = ntail

				m.ctrl[g][s] = int8(lo)
				m.sinces[g][s] = uint16(time.Since(m.startTime) / UnitTime)
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

func (m *LRUMap) Delete(l uint64, key []byte) (ok bool) {
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

func (m *LRUMap) Clear() {
	for i, c := range m.ctrl {
		for j := range c {
			m.ctrl[i][j] = empty
		}
	}
	for i, c := range m.sinces {
		for j := range c {
			m.sinces[i][j] = 0
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

func (m *LRUMap) QueryCount() (count uint64) {
	return m.queryCnt.Load()
}

func (m *LRUMap) MissCount() (count uint64) {
	return m.missCnt.Load()
}

func (m *LRUMap) Count() int {
	return int(m.resident - m.dead)
}

func (m *LRUMap) Capacity() int {
	return int(m.limit - m.resident)
}

//go:inline
func (m *LRUMap) nextSize() (n uint32) {
	n = uint32(math.Ceil(float64(len(m.groups)) * 1.2))
	if m.dead >= (m.resident / 2) {
		n = uint32(len(m.groups))
	}
	return
}

func (m *LRUMap) rehash() {
	n := m.nextSize()
	groups := make([]group, n)
	ctrl := make([]metadata, n)
	sinces := make([]since, n)
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
					sinces[gN][sN] = m.sinces[g][s]
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
	m.sinces = sinces
	m.kvHolder = kvholder
	m.limit = n * maxAvgGroupLoad
	m.resident, m.dead = resident, 0
	m.rehashLock.Unlock()
}

func (m *LRUMap) loadFactor() float32 {
	slots := float32(len(m.groups) * groupSize)
	return float32(m.resident-m.dead) / slots
}

func (m *LRUMap) Eliminate() (delCount int, skipReason int) {
	qc := m.QueryCount()
	if qc > 0 && float32(m.MissCount())/float32(qc) < eliminateMissRate {
		skipReason = skipReason1
		return
	}

	usedRate := m.itemsMemUsage()
	if usedRate < eliminateStart {
		skipReason = skipReason2
		return
	}

	n := int(math.Ceil(float64(float32(m.kvHolder.items) * (eliminateStart - eliminateEnd) / eliminateStart)))
	if n == 0 {
		skipReason = skipReason3
		return
	}

	m.putLock.Lock()
	var item []*Item[uint16]
	item, m.minTopSince = BuildMinTopSince[uint16](m.ctrl, m.sinces, n)

	for i := range item {
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

	m.putLock.Unlock()
	return
}

func (m *LRUMap) GCCopy() (deadCount int, gcMem int, subSince bool, skipReason int) {
	if m.garbageUsage() < garbageRate {
		skipReason = skipReason1
		return
	}

	if m.rehashing {
		skipReason = skipReason2
		return
	} else {
		m.rehashing = true
	}
	oldUsed := m.kvHolder.tail
	deadCount = int(m.dead)
	n := uint32(len(m.groups))
	groups := make([]group, n)
	ctrl := make([]metadata, n)
	sinces := make([]since, n)
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
					sinces[gN][sN] = m.sinces[g][s]
					break
				}
				gN++
				if gN >= uint32(len(groups)) {
					gN = 0
				}
			}
		}
	}

	if time.Since(m.lastSubTime) > LRUSubDuration && m.minTopSince > 0 {
		m.lastSubTime = time.Now()
		var level [16]uint16
		for i := 0; i < 16; i++ {
			level[i] = m.minTopSince
		}
		ctrLen := len(m.ctrl)
		for i := 0; i < ctrLen; i++ {
			simd.MSubs256epu16(unsafe.Pointer(&(sinces[i])), unsafe.Pointer(&level), unsafe.Pointer(&(sinces[i])))
		}
		m.startTime = m.startTime.Add(time.Duration(m.minTopSince) * UnitTime)
		subSince = true
	}

	m.rehashLock.Lock()
	m.groups = groups
	m.ctrl = ctrl
	m.sinces = sinces
	m.kvHolder.buffer.release()
	m.kvHolder = kvholder
	m.resident, m.dead = m.resident-m.dead, 0
	m.rehashLock.Unlock()
	m.putLock.Unlock()
	m.rehashing = false
	gcMem = int(oldUsed - m.kvHolder.tail)
	return
}
