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
	"sync"
	"unsafe"

	"github.com/zuoyebang/bitalostored/butils/vectormap/manual"
)

type kIdx uint32

const (
	IdxTypeMask         uint32 = 0x80000000
	IdxCapOrBigSizeMask uint32 = 0x7f000000
	IdxSmallSizeMask    uint32 = 0xff000000
	IdxOffsetMask       uint32 = 0x00ffffff
)

//go:inline
func (ki *kIdx) valType() uint32 {
	return (uint32(*ki) & IdxTypeMask) >> 31
}

//go:inline
func (ki *kIdx) capOrBigSize() uint32 {
	return (uint32(*ki) & IdxCapOrBigSizeMask) >> 24
}

//go:inline
func (ki *kIdx) offset() uint32 {
	return uint32(*ki) & IdxOffsetMask
}

type kvHolder struct {
	mutex   sync.RWMutex
	tail    uint32
	cap     uint32
	valUsed uint32
	items   uint32
	data    []byte
	buffer  *Buffer
}

func newKVHolder(size Byte) (hdr *kvHolder) {
	b := manual.New(bufferSize + int(size))
	bf := (*Buffer)(unsafe.Pointer(&b[0]))
	bf.buf = b[bufferSize:]
	bf.ref.init(1)
	hdr = &kvHolder{data: b, buffer: bf}
	hdr.tail = uint32(bufferSize)
	hdr.cap = uint32(size)
	return
}

func (hdr *kvHolder) getValue(vOffset, vSize uint32) (v []byte, close func()) {
	hdr.buffer.acquire()
	return hdr.data[vOffset : vOffset+vSize], hdr.buffer.release
}

func (hdr *kvHolder) getKVUnlock(ki kIdx) (k, v []byte) {
	if ki == 0 {
		return nil, nil
	}
	kOffset := ki.offset() * 4
	kEnd := kOffset + 16
	k = hdr.data[kOffset:kEnd]

	vHeader := LoadUint32(unsafe.Pointer(&hdr.data[kEnd]))
	vType := ki.valType()
	if vType == 0 {
		vOffset := (vHeader & IdxOffsetMask) * 4
		vSize := (vHeader & IdxSmallSizeMask) >> 24
		v = hdr.data[vOffset : vOffset+vSize]
		return
	} else {
		vOffset := (vHeader & IdxOffsetMask) * 4
		vBig := ki.capOrBigSize()
		vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
		v = hdr.data[vOffset : vOffset+vSize]
		return
	}
}

func (hdr *kvHolder) getKV(ki kIdx) (k, v []byte) {
	if ki == 0 {
		return nil, nil
	}
	kOffset := ki.offset() * 4
	kEnd := kOffset + 16
	k = hdr.data[kOffset:kEnd]

	vHeader := LoadUint32(unsafe.Pointer(&hdr.data[kEnd]))
	vType := ki.valType()
	if vType == 0 {
		vOffset := (vHeader & IdxOffsetMask) * 4
		vSize := (vHeader & IdxSmallSizeMask) >> 24
		v = make([]byte, vSize)
		hdr.mutex.RLock()
		copy(v, hdr.data[vOffset:vOffset+vSize])
		hdr.mutex.RUnlock()
	} else {
		vOffset := (vHeader & IdxOffsetMask) * 4
		vBig := ki.capOrBigSize()
		vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
		v = hdr.data[vOffset : vOffset+vSize]
	}
	return
}

func (hdr *kvHolder) getKey(ki kIdx) (k []byte) {
	if ki == 0 {
		return nil
	}
	kOffset := ki.offset() * 4
	k = hdr.data[kOffset : kOffset+16]
	return
}

func (hdr *kvHolder) set(k, v []byte) (ki kIdx, fail bool) {
	lv := uint32(len(v))
	if lv >= 1<<7 {
		vCap := Cap4Size(lv)
		ntail := hdr.tail + 20 + vCap
		if ntail > hdr.cap {
			return 0, true
		}
		vBig := lv >> 8
		vSmall := lv & 0xff
		ki = kIdx(hdr.tail/4 + vBig<<24 + 1<<31)
		kEnd := hdr.tail + 16
		copy(hdr.data[hdr.tail:], k)
		vOffset := kEnd + 4
		StoreUint32(unsafe.Pointer(&(hdr.data[kEnd])), vOffset/4+(vSmall<<24))
		copy(hdr.data[vOffset:], v)
		hdr.items++
		hdr.valUsed += vCap
		hdr.tail = ntail
		return
	} else {
		vCap := Cap4Size(lv)
		ntail := hdr.tail + 20 + vCap
		if ntail > hdr.cap {
			return 0, true
		}
		vSmall := lv
		ki = kIdx(hdr.tail/4 + vCap/4<<24)
		kEnd := hdr.tail + 16
		copy(hdr.data[hdr.tail:], k)
		vOffset := kEnd + 4
		StoreUint32(unsafe.Pointer(&(hdr.data[kEnd])), vOffset/4+(vSmall<<24))
		copy(hdr.data[vOffset:], v)
		hdr.items++
		hdr.valUsed += vCap
		hdr.tail = ntail
		return
	}
}

func (hdr *kvHolder) gcSet(k, v []byte) (ki kIdx, fail bool) {
	lv := uint32(len(v))
	if lv >= 1<<7 {
		vCap := Cap4Size(lv)
		ntail := hdr.tail + 20 + vCap
		if ntail > hdr.cap {
			return 0, true
		}
		vBig := lv >> 8
		vSmall := lv & 0xff
		ki = kIdx(hdr.tail/4 + vBig<<24 + 1<<31)
		kEnd := hdr.tail + 16
		copy(hdr.data[hdr.tail:], k)
		vOffset := kEnd + 4
		vHeader := vOffset/4 + (vSmall << 24)
		copyUint32(unsafe.Pointer(&(hdr.data[kEnd])), unsafe.Pointer(&vHeader), 0, 4)
		copy(hdr.data[vOffset:], v)
		hdr.items++
		hdr.valUsed += vCap
		hdr.tail = ntail
		return
	} else {
		vCap := Cap4Size(lv)
		ntail := hdr.tail + 20 + vCap
		if ntail > hdr.cap {
			return 0, true
		}
		vSmall := lv
		ki = kIdx(hdr.tail/4 + vCap/4<<24)
		kEnd := hdr.tail + 16
		copy(hdr.data[hdr.tail:], k)
		vOffset := kEnd + 4
		vHeader := vOffset/4 + (vSmall << 24)
		copyUint32(unsafe.Pointer(&(hdr.data[kEnd])), unsafe.Pointer(&vHeader), 0, 4)
		copy(hdr.data[vOffset:], v)
		hdr.items++
		hdr.valUsed += vCap
		hdr.tail = ntail
		return
	}
}

func (hdr *kvHolder) update(ki kIdx, v []byte) (kIdx, bool) {
	kOffset := ki.offset() * 4
	kEnd := kOffset + 16
	vHeader := LoadUint32(unsafe.Pointer(&hdr.data[kEnd]))
	vType := ki.valType()
	lv := uint32(len(v))
	if lv >= 1<<15 {
		hdr.items--
		if vType == 0 {
			hdr.valUsed -= ki.capOrBigSize()
		} else {
			vBig := ki.capOrBigSize()
			vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
			hdr.valUsed -= Cap4Size(vSize)
		}
		return 0, true
	} else if lv >= 1<<7 {
		vCap := Cap4Size(lv)
		ntail := hdr.tail + vCap
		if vType == 0 {
			hdr.valUsed -= ki.capOrBigSize()
		} else {
			vBig := ki.capOrBigSize()
			vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
			hdr.valUsed -= Cap4Size(vSize)
		}
		if ntail > hdr.cap {
			hdr.items--
			return 0, true
		}
		vBig := lv & 0x7f00 >> 8
		vSmall := uint32(lv) & 0xff
		ki = kIdx(kOffset/4 + vBig<<24 + 1<<31)
		copy(hdr.data[hdr.tail:], v)
		StoreUint32(unsafe.Pointer(&(hdr.data[kEnd])), hdr.tail/4+vSmall<<24)
		hdr.tail = ntail
		hdr.valUsed += vCap
		return ki, false
	} else if vType == 0 && lv <= ki.capOrBigSize()*4 && lv < 1<<7 {
		vHeader := LoadUint32(unsafe.Pointer(&hdr.data[kEnd]))
		vOffset := vHeader & IdxOffsetMask
		hdr.mutex.Lock()
		StoreUint32(unsafe.Pointer(&(hdr.data[kEnd])), vOffset+lv<<24)
		copy(hdr.data[vOffset*4:], v)
		hdr.mutex.Unlock()
		return ki, false
	} else {
		vCap := Cap4Size(lv)
		ntail := hdr.tail + vCap
		if vType == 0 {
			hdr.valUsed -= ki.capOrBigSize()
		} else {
			vBig := ki.capOrBigSize()
			vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
			hdr.valUsed -= Cap4Size(vSize)
		}
		if ntail > hdr.cap {
			hdr.items--
			return 0, true
		}
		ki = kIdx(kOffset/4 + vCap/4<<24)
		copy(hdr.data[hdr.tail:], v)
		StoreUint32(unsafe.Pointer(&(hdr.data[kEnd])), hdr.tail/4+(lv<<24))
		hdr.tail = ntail
		hdr.valUsed += vCap
		return ki, false
	}
}

func (hdr *kvHolder) updateMultiVal(ki kIdx, lv uint32, vs [][]byte) (kIdx, bool) {
	kOffset := ki.offset() * 4
	kEnd := kOffset + 16
	vType := ki.valType()
	if lv >= 1<<15 {
		return 0, true
	} else if lv >= 1<<7 {
		vCap := Cap4Size(lv)
		ntail := hdr.tail + vCap
		if ntail > hdr.cap {
			return 0, true
		}
		vBig := lv & 0x7f00 >> 8
		vSmall := uint32(lv) & 0xff
		ki = kIdx(kOffset/4 + vBig<<24 + 1<<31)
		StoreUint32(unsafe.Pointer(&(hdr.data[kEnd])), hdr.tail/4+vSmall<<24)
		for _, v := range vs {
			copy(hdr.data[hdr.tail:], v)
			hdr.tail += uint32(len(v))
		}
		hdr.tail = ntail
		return ki, false
	} else if vType == 0 && lv <= ki.capOrBigSize()*4 && lv < 1<<7 {
		vHeader := LoadUint32(unsafe.Pointer(&hdr.data[kEnd]))
		vOffset := vHeader & IdxOffsetMask
		StoreUint32(unsafe.Pointer(&(hdr.data[kEnd])), vOffset+lv<<24)
		hdr.mutex.Lock()
		idx := vOffset * 4
		for _, v := range vs {
			copy(hdr.data[idx:], v)
			idx += uint32(len(v))
		}
		hdr.mutex.Unlock()
		return ki, false
	} else {
		vCap := Cap4Size(lv)
		ntail := hdr.tail + vCap
		if ntail > hdr.cap {
			return 0, true
		}
		ki = kIdx(kOffset/4 + vCap/4<<24)
		StoreUint32(unsafe.Pointer(&(hdr.data[kEnd])), hdr.tail/4+(lv<<24))
		for _, v := range vs {
			copy(hdr.data[hdr.tail:], v)
			hdr.tail += uint32(len(v))
		}
		hdr.tail = ntail
		return ki, false
	}
}

func (hdr *kvHolder) del(ki kIdx) {
	if ki == 0 {
		return
	}
	kEnd := ki.offset()*4 + 16
	vType := ki.valType()
	vHeader := LoadUint32(unsafe.Pointer(&hdr.data[kEnd]))
	if vType == 0 {
		vSize := (vHeader & IdxSmallSizeMask) >> 24
		vCap := Cap4Size(vSize)
		hdr.valUsed -= vCap
		hdr.items--
		return
	} else {
		vBig := ki.capOrBigSize()
		vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
		vCap := Cap4Size(vSize)
		hdr.valUsed -= vCap
		hdr.items--
		return
	}
}

//go:inline
func (hdr *kvHolder) memUsage() (usage float32) {
	usage = float32(hdr.tail) / float32(hdr.cap)
	return
}

//go:inline
func (hdr *kvHolder) itemsMemUsage() (usage float32) {
	usage = float32(hdr.valUsed+hdr.items*20+uint32(bufferSize)) / float32(hdr.cap)
	return
}

//go:inline
func (hdr *kvHolder) memUseRate() (usage float32) {
	usage = float32(hdr.valUsed+hdr.items*20+uint32(bufferSize)) / float32(hdr.tail)
	return
}

//go:inline
func copyUint64(dest, src *uint64, s, e int) {
	sb := (*[8]byte)(unsafe.Pointer(src))
	db := (*[8]byte)(unsafe.Pointer(dest))
	copy((*db)[s:e], (*sb)[s:e])
}

//go:inline
func copyUint32(dest, src unsafe.Pointer, s, e int) {
	sb := (*[4]byte)(src)
	db := (*[4]byte)(dest)
	copy((*db)[s:e], (*sb)[s:e])
}

func StoreUint32(dest unsafe.Pointer, src uint32) {
	sb := (*[4]byte)(unsafe.Pointer(&src))
	db := (*[4]byte)(dest)
	copy((*db)[:], (*sb)[:])
}

func LoadUint32(src unsafe.Pointer) (dest uint32) {
	sb := (*[4]byte)(src)
	db := (*[4]byte)(unsafe.Pointer(&dest))
	copy((*db)[:], (*sb)[:])
	return
}

//go:inline
func Cap4Size(vSize uint32) uint32 {
	if vSize&3 != 0 {
		return (vSize>>2 + 1) << 2
	}
	return vSize
}
