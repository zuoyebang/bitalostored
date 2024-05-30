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

package vectormap

import (
	"fmt"
	"math"
	"time"

	"github.com/zuoyebang/bitalostored/butils/md5hash"
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

const (
	maxLoadFactor        float32 = float32(maxAvgGroupLoad) / float32(groupSize)
	MaxUint64            uint64  = 1<<64 - 1
	MaxUint32            uint32  = 1<<32 - 1
	maxCount             uint8   = 200
	maxBuckets           int     = 4096
	minBuckets           int     = 1024
	maxMemSize           Byte    = 128 << 30
	minMemSize           Byte    = 1 << 30
	overShortSize        uint32  = 1 << 7
	overLongSize         uint32  = (1 << 15) - 1
	overLongStoreH       uint32  = overLongSize >> 8
	overLongStoreL       uint32  = overLongSize & 0xff
	overLongStoreHeaderH uint32  = overLongStoreH << 24
	overLongStoreHeaderL uint32  = overLongStoreL << 24
	mapTypeHeader        uint32  = 1 << 31
	limitSize            uint32  = 4 << 20
	storeUintBytes       uint32  = 4

	MinEliminateDuration = 60 * time.Second
)

const (
	skipReason1 = 1
	skipReason2 = 2
	skipReason3 = 4
	skipReason4 = 8
	skipReason5 = 16
)

type defaultLogger struct {
}

func (l *defaultLogger) Debugf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}
func (l *defaultLogger) Infof(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}
func (l *defaultLogger) Warnf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}
func (l *defaultLogger) Errorf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

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

func WithLRUUnitTime(unitTime time.Duration) Option {
	return func(vm *VectorMap) {
		UnitTime = unitTime
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

func WithEliminate(memCap Byte, goroutines int, duration time.Duration) Option {
	return func(vm *VectorMap) {
		vm.memCap = memCap

		if vm.debug {
			if goroutines == 0 {
				return
			}
		}
		if goroutines <= 0 {
			goroutines = 1
		}
		if !vm.debug && duration < MinEliminateDuration {
			duration = MinEliminateDuration
		}

		vm.eliminateHandler = &eliminateHandler{
			goroutines:     goroutines,
			circleDuration: time.Duration(float64(duration) * 0.15),
			stepDuration:   duration / 1000,
		}
	}
}

type MapType uint8

const (
	MapTypeLFU MapType = iota
	MapTypeLRU
)

func WithType(mtyp MapType) Option {
	return func(vm *VectorMap) {
		vm.mtype = mtyp
	}
}

type VectorMap struct {
	buckets          int
	shards           []Map
	globalMask       uint64
	reputFails       uint64
	memCap           Byte
	eliminateHandler *eliminateHandler
	logger           ILogger
	debug            bool
	mtype            MapType
}

func NewVectorMap(sz uint32, ops ...Option) (vm *VectorMap) {
	vm = &VectorMap{}
	for _, op := range ops {
		op(vm)
	}

	if !vm.debug {
		if vm.memCap < minMemSize {
			vm.memCap = minMemSize
		} else if vm.memCap > maxMemSize {
			vm.memCap = maxMemSize
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

	vm.shards = make([]Map, vm.buckets)
	vm.globalMask = globalMask

	switch vm.mtype {
	case MapTypeLRU:
		for i := range vm.shards {
			vm.shards[i] = newInnerLRUMap(vm, c)
		}
	case MapTypeLFU:
		for i := range vm.shards {
			vm.shards[i] = newInnerLFUMap(vm, c)
		}
	}

	if vm.eliminateHandler != nil {
		vm.eliminateHandler.stepDuration = time.Duration(int(vm.eliminateHandler.stepDuration) * (vm.buckets / 1000))
		vm.eliminateHandler.Handle(vm)
	}
	return vm
}

//go:inline
func (vm *VectorMap) slotAt(hi uint64) Map {
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

func (vm *VectorMap) RePutFails() uint64 {
	return vm.reputFails
}

func (vm *VectorMap) RePut(k []byte, v []byte) (res bool) {
	defer func() {
		if !res {
			vm.reputFails++
		}
	}()
	if len(v) >= int(limitSize) {
		res = false
		return
	}
	h, hi, lo := md5hash.MD5Sum(k)
	res = vm.slotAt(hi).RePut(lo, h, v)
	return
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
		sum += m.Items()
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
		count += m.QueryCount()
	}
	return
}

func (vm *VectorMap) MissCount() (count uint64) {
	for _, m := range vm.shards {
		count += m.MissCount()
	}
	return
}

func (vm *VectorMap) MaxMem() Byte {
	return vm.memCap
}

func (vm *VectorMap) UsedMem() (usedMem Byte) {
	for _, m := range vm.shards {
		usedMem += m.UsedMem()
	}
	return
}

func (vm *VectorMap) EffectiveMem() (usedMem Byte) {
	for _, m := range vm.shards {
		usedMem += m.ItemsUsedMem()
	}
	return
}

type Map interface {
	Put(uint64, []byte, []byte) bool
	PutMultiValue(uint64, []byte, uint32, [][]byte) bool
	RePut(uint64, []byte, []byte) bool
	Get(uint64, []byte) ([]byte, func(), bool)
	Delete(uint64, []byte) bool
	Has(uint64, []byte) bool
	Items() uint32
	UsedMem() Byte
	ItemsUsedMem() Byte
	itemsMemUsage() float32
	memUsage() float32
	Clear()
	Count() int
	Capacity() int
	QueryCount() uint64
	MissCount() uint64
	Eliminate() (delCount int, skipReason int)
	GCCopy() (deadCount int, gcMem int, subSince bool, skipReason int)
	kvholder() *kvHolder
	Groups() []group
	Resident() uint32
	Dead() uint32
}

type metadata [groupSize]int8
type counter [groupSize]uint8
type since [groupSize]uint16
type group [groupSize]kIdx

const (
	h1Mask    uint64 = 0xffff_ffff_ffff_ff80
	h2Mask    uint64 = 0x0000_0000_0000_007f
	empty     int8   = -128 // 0b1000_0000
	tombstone int8   = -2   // 0b1111_1110
)

type h1 uint64
type h2 int8

const (
	eliminateStart    = 0.95
	eliminateEnd      = 0.9
	eliminateMissRate = 0.1
	garbageRate       = 0.045
	maxMemUsage       = 0.999
)

type eliminateHandler struct {
	goroutines     int
	circleDuration time.Duration
	stepDuration   time.Duration
}

func (h *eliminateHandler) Handle(vm *VectorMap) {
	switch vm.mtype {
	case MapTypeLFU:
		for i := 0; i < h.goroutines; i++ {
			go func(idx int) {
				for {
					start := time.Now()
					var eliMaps, eliItems, gcMaps, gcItems, gcMem, eliSkipReason, gcSkipReason int
					for j := idx; j < vm.buckets; j += h.goroutines {
						ec, reason := vm.shards[j].Eliminate()
						if ec > 0 {
							eliMaps++
							eliItems += ec
						}
						eliSkipReason |= reason
						gcI, gcM, _, rs := vm.shards[j].GCCopy()
						if gcI > 0 {
							gcMaps++
							gcItems += gcI
							gcMem += gcM
						}
						gcSkipReason |= rs
						time.Sleep(h.stepDuration)
					}
					cost := time.Since(start)
					if vm.logger != nil {
						vm.logger.Infof("eliminate index %d cost: %v, eliMaps: %d, eliItems: %d, gcMaps: %d, gcItems: %d, gcMem: %d",
							idx, cost, eliMaps, eliItems, gcMaps, gcItems, gcMem)
					}
					time.Sleep(h.circleDuration)
				}
			}(i)
		}
	case MapTypeLRU:
		for i := 0; i < h.goroutines; i++ {
			go func(idx int) {
				for {
					start := time.Now()
					var eliMaps, eliItems, gcMaps, gcItems, gcMem, subTimes, eliSkipReason, gcSkipReason int
					var minStartTime = time.Now()
					var topSince uint16
					for j := idx; j < vm.buckets; j += h.goroutines {
						ec, reason := vm.shards[j].Eliminate()
						if ec > 0 {
							eliMaps++
							eliItems += ec
						}
						eliSkipReason |= reason
						gcI, gcM, subSince, rs := vm.shards[j].GCCopy()
						if gcI > 0 {
							gcMaps++
							gcItems += gcI
							gcMem += gcM
							if subSince {
								subTimes++
							}
						}
						lruMap := vm.shards[j].(*LRUMap)
						if lruMap.startTime.Before(start) {
							minStartTime = lruMap.startTime
						}
						if lruMap.minTopSince > topSince {
							topSince = lruMap.minTopSince
						}
						gcSkipReason |= rs
						time.Sleep(h.stepDuration)
					}
					cost := time.Since(start)
					if vm.logger != nil {
						vm.logger.Infof("eliminate index %d cost: %v, eliMaps: %d, eliItems: %d, gcMaps: %d, gcItems: %d, gcMem: %d, minStartTime: %s, subTimes: %d, topSince: %d, eliSkipReason: %d, gcSkipReason: %d",
							idx, cost, eliMaps, eliItems, gcMaps, gcItems, gcMem, minStartTime.Format(time.DateTime), subTimes, topSince, eliSkipReason, gcSkipReason)
					}
					time.Sleep(h.circleDuration)
				}
			}(i)
		}
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
