package base

import (
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

const bitmapItemMax = 2048
const bitmapFlushSecond = 2700

type BitmapItem struct {
	key   []byte
	khash uint32
	mu    struct {
		sync.RWMutex
		rb *roaring64.Bitmap
	}
	expireMs atomic.Uint64 // millisecond
	modify   atomic.Int64  // second
}

type BitmapMem struct {
	enable bool

	mu struct {
		sync.RWMutex
		items map[string]*BitmapItem
		count int
	}

	migrating   atomic.Bool
	migrateSlot atomic.Uint32

	baseDB      *BaseDB
	flushing    atomic.Bool
	fasting     bool
	flushLock   sync.Mutex
	flushSecond int64
	scanItems   []*BitmapItem
	closeCh     chan struct{}
	wg          sync.WaitGroup
}

func NewBitmapItem(key []byte, khash uint32, rb *roaring64.Bitmap, timestamp uint64) *BitmapItem {
	bi := &BitmapItem{
		key:   key,
		khash: khash,
	}
	bi.mu.rb = rb
	bi.expireMs.Store(timestamp)
	now := tclock.GetTimestampSecond()
	bi.modify.Store(now)
	return bi
}

func (bi *BitmapItem) GetReader() (*roaring64.Bitmap, func()) {
	if bi.Expired() {
		return nil, nil
	} else {
		bi.mu.RLock()
		return bi.mu.rb, func() {
			bi.mu.RUnlock()
		}
	}
}

func (bi *BitmapItem) Expired() bool {
	expire := bi.expireMs.Load()
	if expire == 0 {
		return false
	} else {
		return expire <= uint64(tclock.GetTimestampMilli())
	}
}

func (bi *BitmapItem) GetWriter() (*roaring64.Bitmap, func()) {
	bi.mu.Lock()
	if bi.Expired() {
		bi.reset()
	}
	now := tclock.GetTimestampSecond()
	bi.modify.Store(now)
	return bi.mu.rb, func() {
		bi.mu.Unlock()
	}
}

func (bi *BitmapItem) reset() {
	bi.mu.rb = roaring64.NewBitmap()
	bi.expireMs.Store(0)
}

func (bi *BitmapItem) SetExpire(expire uint64) {
	bi.expireMs.Store(expire)
	bi.modify.Store(tclock.GetTimestampSecond())
}

func NewBitmapMem(db *BaseDB) *BitmapMem {
	bm := &BitmapMem{
		baseDB:      db,
		flushSecond: bitmapFlushSecond,
		scanItems:   make([]*BitmapItem, 0, bitmapItemMax),
		closeCh:     make(chan struct{}),
		flushLock:   sync.Mutex{},
	}

	bm.mu.items = make(map[string]*BitmapItem, 10)
	bm.wg.Add(1)
	go bm.RunFlushWorker()
	return bm
}

func (bm *BitmapMem) SetEnable() {
	if !bm.enable {
		bm.enable = true
	}
}

func (bm *BitmapMem) GetEnable() bool {
	return bm.enable
}

func (bm *BitmapMem) Get(key []byte) (*BitmapItem, bool) {
	if !bm.enable {
		return nil, false
	}

	bm.mu.RLock()
	defer bm.mu.RUnlock()

	if v, ok := bm.mu.items[unsafe2.String(key)]; ok {
		return v, true
	} else {
		return nil, false
	}
}

func (bm *BitmapMem) AddItem(key []byte, khash uint32, newBi func(k []byte) *BitmapItem) bool {
	if bm.flushing.Load() {
		return false
	}

	if bm.migrating.Load() && khash%utils.TotalSlot == bm.migrateSlot.Load() {
		return false
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()

	if bm.IsFull() {
		return false
	}

	keyStr := string(key)
	bm.mu.items[keyStr] = newBi(unsafe2.ByteSlice(keyStr))
	bm.mu.count++
	return true
}

func (bm *BitmapMem) Delete(key []byte, deleteDB bool) (bool, error) {
	if !bm.enable {
		return false, nil
	}

	bm.mu.RLock()
	_, exist := bm.mu.items[unsafe2.String(key)]
	bm.mu.RUnlock()
	if !exist {
		return false, nil
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()
	return bm.doDeleteKey(key, deleteDB)
}

func (bm *BitmapMem) deleteItem(it *BitmapItem, deleteDB bool) (bool, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	return bm.doDeleteItem(it, deleteDB)
}

func (bm *BitmapMem) checkItem(it *BitmapItem) bool {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	return bm.doCheckItem(it)
}

func (bm *BitmapMem) Close() {
	close(bm.closeCh)
	bm.wg.Wait()
	log.Infof("bitmap mem closed")
}

func (bm *BitmapMem) StartMigrate(slotId uint32) {
	bm.migrateSlot.Store(slotId)
	bm.migrating.Store(true)
	bm.flushSlot(slotId)
}

func (bm *BitmapMem) ClearMigrate() {
	bm.migrating.Store(false)
}

func (bm *BitmapMem) RunFlushWorker() {
	log.Infof("bitmap flush starts to work")
	defer func() {
		bm.wg.Done()
		log.Infof("bitmap flush closed")
	}()

	worker := func() (closed bool) {
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("bitmap flush panic err:%v stack=%s", r, string(debug.Stack()))
			}
		}()

		tick := time.NewTicker(time.Duration(bm.flushSecond) * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				now := tclock.GetTimestampSecond()
				bm.Flush(false)
				bm.Evict(now)
			case <-bm.closeCh:
				closed = true
				bm.Flush(true)
				return
			}
		}
	}
	for {
		if worker() {
			break
		}
	}
}

func (bm *BitmapMem) Flush(fast bool) {
	if !bm.enable {
		return
	}

	if fast {
		bm.fasting = true
	}
	defer func() {
		if fast {
			bm.fasting = false
		}
	}()

	bm.flushLock.Lock()
	defer bm.flushLock.Unlock()

	bm.flushing.Store(true)
	defer bm.flushing.Store(false)

	now := tclock.GetTimestampSecond()
	bm.scanItems = bm.scanItems[:0]
	bm.mu.RLock()
	for _, it := range bm.mu.items {
		bm.scanItems = append(bm.scanItems, it)
	}
	bm.mu.RUnlock()
	total := len(bm.scanItems)

	var expireNum, nullNum, flushNum, flushBytes int
	var meta [MetaStringValueLen]byte
	for _, it := range bm.scanItems {
		if bm.fasting != fast {
			break
		}

		if it.Expired() {
			bm.deleteItem(it, true)
			expireNum++
			continue
		}

		if now-it.modify.Load() >= 2*bm.flushSecond {
			bm.deleteItem(it, false)
			continue
		}

		it.mu.RLock()
		if it.mu.rb.IsEmpty() {
			nullNum++
			it.mu.RUnlock()
			bm.deleteItem(it, true)
			continue
		}

		val, err := it.mu.rb.MarshalBinary()
		it.mu.RUnlock()
		if err != nil {
			continue
		}

		if !bm.checkItem(it) {
			continue
		}

		flushNum++
		ek, ekCloser := EncodeMetaKey(it.key, it.khash)
		EncodeMetaDbValueForString(meta[:], it.expireMs.Load())
		vlen := MetaStringValueLen + len(val)
		bm.baseDB.SetMetaDataByValues(ek, vlen, meta[:], val)
		ekCloser()
		flushBytes += vlen

		if !fast {
			time.Sleep(500 * time.Millisecond)
		}
	}
	log.Infof("bitmap item flush. cost:%d(s) total:%d expireNum:%d nullNum:%d flushNum:%d flushBytes:%d", tclock.GetTimestampSecond()-now, total, expireNum, nullNum, flushNum, flushBytes)
}

func (bm *BitmapMem) IsFull() bool {
	return bm.mu.count >= bitmapItemMax
}

func (bm *BitmapMem) doDeleteKey(key []byte, deleteDB bool) (bool, error) {
	var err error
	keyStr := unsafe2.String(key)
	if v, ok := bm.mu.items[keyStr]; ok {
		delete(bm.mu.items, keyStr)
		bm.mu.count--

		if deleteDB {
			ek, ekCloser := EncodeMetaKey(v.key, v.khash)
			defer ekCloser()
			err = bm.baseDB.DeleteMetaKey(ek)
		}
		return true, err
	} else {
		return false, nil
	}
}

func (bm *BitmapMem) doDeleteItem(it *BitmapItem, deleteDB bool) (bool, error) {
	var err error
	keyStr := unsafe2.String(it.key)
	if v, ok := bm.mu.items[keyStr]; ok && v == it {
		delete(bm.mu.items, keyStr)
		bm.mu.count--

		if deleteDB {
			ek, ekCloser := EncodeMetaKey(v.key, v.khash)
			defer ekCloser()
			err = bm.baseDB.DeleteMetaKey(ek)
		}
		return true, err
	}
	return false, nil
}

func (bm *BitmapMem) doCheckItem(it *BitmapItem) bool {
	if v, ok := bm.mu.items[unsafe2.String(it.key)]; ok && v == it {
		return true
	}
	return false
}

func (bm *BitmapMem) Evict(modifyTime int64) {
	bm.mu.RLock()
	if !bm.IsFull() {
		bm.mu.RUnlock()
		return
	}

	bm.scanItems = bm.scanItems[:0]
	for _, it := range bm.mu.items {
		if it.modify.Load() < modifyTime {
			bm.scanItems = append(bm.scanItems, it)
		}
	}
	bm.mu.RUnlock()

	sort.Slice(bm.scanItems, func(i, j int) bool {
		if bm.scanItems[i].modify.Load() <= bm.scanItems[j].modify.Load() {
			return true
		} else {
			return false
		}
	})

	evictMax := bitmapItemMax * 3 / 10
	evictCount := 0
	for _, it := range bm.scanItems {
		if it.modify.Load() >= modifyTime {
			continue
		}
		ok, _ := bm.deleteItem(it, false)
		if ok {
			evictCount++
		}
		if evictCount >= evictMax {
			break
		}
	}
	log.Infof("bitmap evict itemNum:%d", evictCount)
}

func (bm *BitmapMem) flushSlot(slotId uint32) {
	if !bm.enable {
		return
	}

	bm.flushLock.Lock()
	defer bm.flushLock.Unlock()

	bm.scanItems = bm.scanItems[:0]
	bm.mu.RLock()
	for _, it := range bm.mu.items {
		if it.khash%utils.TotalSlot == slotId {
			bm.scanItems = append(bm.scanItems, it)
		}
	}
	bm.mu.RUnlock()

	var meta [MetaStringValueLen]byte
	for _, it := range bm.scanItems {
		bm.mu.Lock()
		if it.Expired() {
			bm.doDeleteItem(it, true)
			bm.mu.Unlock()
			continue
		}

		it.mu.RLock()
		if it.mu.rb.IsEmpty() {
			it.mu.RUnlock()
			bm.doDeleteItem(it, true)
			bm.mu.Unlock()
			continue
		}

		val, err := it.mu.rb.MarshalBinary()
		it.mu.RUnlock()
		if err != nil {
			log.Errorf("migrate flush bitmap err:%s key:%s", err, it.key)
			bm.doDeleteItem(it, false)
			bm.mu.Unlock()
			continue
		}

		if !bm.doCheckItem(it) {
			bm.mu.Unlock()
			continue
		}

		ek, ekCloser := EncodeMetaKey(it.key, it.khash)
		EncodeMetaDbValueForString(meta[:], it.expireMs.Load())
		vlen := MetaStringValueLen + len(val)
		bm.baseDB.SetMetaDataByValues(ek, vlen, meta[:], val)
		ekCloser()
		bm.doDeleteItem(it, false)
		bm.mu.Unlock()
	}
}
