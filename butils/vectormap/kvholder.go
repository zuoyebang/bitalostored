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
	"encoding/binary"
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
	limit   uint32
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
	hdr.limit = uint32(float32(hdr.cap) * maxMemUsage)
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

	vHeader := LoadUint32(hdr.data[kEnd:])
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
		if vSize == overLongSize {
			vSize = binary.BigEndian.Uint32(hdr.data[vOffset:])
			vOffset += 4
		}
		v = hdr.data[vOffset : vOffset+vSize]
		return
	}
}

func (hdr *kvHolder) getKey(ki kIdx) (k []byte) {
	if ki == 0 {
		return nil
	}
	kOffset := ki.offset() * 4
	k = hdr.data[kOffset : kOffset+16]
	return
}

func (hdr *kvHolder) gcSet(k, v []byte) (ki kIdx, fail bool) {
	lv := uint32(len(v))
	if lv >= overLongSize {
		vCap := Cap4Size(lv) + 4
		ntail := hdr.tail + 20 + vCap
		if ntail > hdr.cap {
			return 0, true
		}
		ki = kIdx(hdr.tail/4 + overLongStoreHeaderH + mapTypeHeader)
		kEnd := hdr.tail + 16
		copy(hdr.data[hdr.tail:], k)
		vOffset := kEnd + 4
		vHeader := vOffset/4 + overLongStoreHeaderL
		StoreUint32(hdr.data[kEnd:], vHeader)
		StoreUint32(hdr.data[vOffset:], lv)
		copy(hdr.data[vOffset+4:], v)
		hdr.items++
		hdr.valUsed += vCap
		hdr.tail = ntail
		return
	} else if lv >= 1<<7 {
		vCap := Cap4Size(lv)
		ntail := hdr.tail + 20 + vCap
		if ntail > hdr.cap {
			return 0, true
		}
		vBig := lv >> 8
		vSmall := lv & 0xff
		ki = kIdx(hdr.tail/4 + vBig<<24 + mapTypeHeader)
		kEnd := hdr.tail + 16
		copy(hdr.data[hdr.tail:], k)
		vOffset := kEnd + 4
		vHeader := vOffset/4 + (vSmall << 24)
		StoreUint32(hdr.data[kEnd:], vHeader)
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
		StoreUint32(hdr.data[kEnd:], vHeader)
		copy(hdr.data[vOffset:], v)
		hdr.items++
		hdr.valUsed += vCap
		hdr.tail = ntail
		return
	}
}

func (hdr *kvHolder) del(ki kIdx) {
	if ki == 0 {
		return
	}
	kEnd := ki.offset()*4 + 16
	vType := ki.valType()
	vHeader := LoadUint32(hdr.data[kEnd:])
	if vType == 0 {
		vSize := (vHeader & IdxSmallSizeMask) >> 24
		vCap := Cap4Size(vSize)
		hdr.valUsed -= vCap
		hdr.items--
		return
	} else {
		vBig := ki.capOrBigSize()
		vSize := vHeader&IdxSmallSizeMask>>24 + vBig<<8
		if vSize == overLongSize {
			vOffset := (vHeader & IdxOffsetMask) * 4
			vSize = LoadUint32(hdr.data[vOffset:])
			hdr.valUsed -= Cap4Size(vSize) + 4
		} else {
			hdr.valUsed -= Cap4Size(vSize)
		}
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
func (hdr *kvHolder) garbageUsage() (usage float32) {
	usage = (float32(hdr.tail) - float32(hdr.valUsed+hdr.items*20+uint32(bufferSize))) / float32(hdr.cap)
	return
}

//go:inline
func (hdr *kvHolder) memUseRate() (usage float32) {
	usage = float32(hdr.valUsed+hdr.items*20+uint32(bufferSize)) / float32(hdr.tail)
	return
}

func StoreUint32(buf []byte, src uint32) {
	binary.BigEndian.PutUint32(buf[0:], src)
}

func LoadUint32(buf []byte) (dest uint32) {
	dest = binary.BigEndian.Uint32(buf[0:])
	return
}

//go:inline
func Cap4Size(vSize uint32) uint32 {
	if vSize&3 != 0 {
		return (vSize>>2 + 1) << 2
	}
	return vSize
}
