package localcache

import (
	"container/list"
	"time"
)

// 淘汰最少使用的数据.
type LFUCache struct {
	baseCache
	items    map[interface{}]*lfuItem
	freqList *list.List // list for freqEntry
}

var _ Cache = (*LFUCache)(nil)

type lfuItem struct {
	clock       Clock
	key         interface{}
	value       interface{}
	freqElement *list.Element
	expiration  *time.Time
}

type freqEntry struct {
	freq  uint
	items map[*lfuItem]struct{}
}

func newLFUCache(cb *CacheBuilder) *LFUCache {
	c := &LFUCache{}
	buildCache(&c.baseCache, cb)

	c.init()
	c.loadGroup.cache = c
	return c
}

func (c *LFUCache) init() {
	c.freqList = list.New()
	c.items = make(map[interface{}]*lfuItem, c.size)
	c.freqList.PushFront(&freqEntry{
		freq:  0,
		items: make(map[*lfuItem]struct{}),
	})
}

// 设置一个新键值对
func (c *LFUCache) Set(key, value interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.set(key, value)
	return err
}

// 设置一个新键值对，并设置过期时间
func (c *LFUCache) SetWithExpire(key, value interface{}, expiration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, err := c.set(key, value)
	if err != nil {
		return err
	}

	t := c.clock.Now().Add(expiration)
	item.(*lfuItem).expiration = &t
	return nil
}

func (c *LFUCache) set(key, value interface{}) (interface{}, error) {
	var err error
	if c.serializeFunc != nil {
		value, err = c.serializeFunc(key, value)
		if err != nil {
			return nil, err
		}
	}

	// 检查是否存在该键值对
	item, ok := c.items[key]
	if ok {
		item.value = value
	} else {
		// 检查是否超过缓存大小
		if len(c.items) >= c.size {
			c.evict(1)
		}
		item = &lfuItem{
			clock:       c.clock,
			key:         key,
			value:       value,
			freqElement: nil,
		}
		el := c.freqList.Front()
		fe := el.Value.(*freqEntry)
		fe.items[item] = struct{}{}

		item.freqElement = el
		c.items[key] = item
	}

	if c.expiration != nil {
		t := c.clock.Now().Add(*c.expiration)
		item.expiration = &t
	}

	if c.addedFunc != nil {
		c.addedFunc(key, value)
	}

	return item, nil
}

// 从缓存池中获取一个值，如果存在则返回.
// 如果键不存在，并且有LoaderFunc，则使用LoaderFunc方法返回值.
func (c *LFUCache) Get(key interface{}) (interface{}, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, true)
	}
	return v, err
}

// 从缓存池中获取一个值，如果存在则返回.
// 如果键不存在，则返回KeyNotFoundError.
// 如果缓存对象有LoaderFunc，则发送请求刷新指定键的值.
func (c *LFUCache) GetIFPresent(key interface{}) (interface{}, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, false)
	}
	return v, err
}

func (c *LFUCache) get(key interface{}, onLoad bool) (interface{}, error) {
	v, err := c.getValue(key, onLoad)
	if err != nil {
		return nil, err
	}
	if c.deserializeFunc != nil {
		return c.deserializeFunc(key, v)
	}
	return v, nil
}

func (c *LFUCache) getValue(key interface{}, onLoad bool) (interface{}, error) {
	c.mu.Lock()
	item, ok := c.items[key]
	if ok {
		if !item.IsExpired(nil) {
			c.increment(item)
			v := item.value
			c.mu.Unlock()
			if !onLoad {
				c.stats.IncrHitCount()
			}
			return v, nil
		}
		c.removeItem(item)
	}
	c.mu.Unlock()
	if !onLoad {
		c.stats.IncrMissCount()
	}
	return nil, KeyNotFoundError
}

func (c *LFUCache) getWithLoader(key interface{}, isWait bool) (interface{}, error) {
	if c.loaderExpireFunc == nil {
		return nil, KeyNotFoundError
	}
	value, _, err := c.load(key, func(v interface{}, expiration *time.Duration, e error) (interface{}, error) {
		if e != nil {
			return nil, e
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		item, err := c.set(key, v)
		if err != nil {
			return nil, err
		}
		if expiration != nil {
			t := c.clock.Now().Add(*expiration)
			item.(*lfuItem).expiration = &t
		}
		return v, nil
	}, isWait)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (c *LFUCache) increment(item *lfuItem) {
	currentFreqElement := item.freqElement
	currentFreqEntry := currentFreqElement.Value.(*freqEntry)
	nextFreq := currentFreqEntry.freq + 1
	delete(currentFreqEntry.items, item)

	// 一个布尔值，判断是否重用当前空条目
	removable := isRemovableFreqEntry(currentFreqEntry)

	// 将条目插入到有效条目中
	nextFreqElement := currentFreqElement.Next()
	switch {
	case nextFreqElement == nil || nextFreqElement.Value.(*freqEntry).freq > nextFreq:
		if removable {
			currentFreqEntry.freq = nextFreq
			nextFreqElement = currentFreqElement
		} else {
			nextFreqElement = c.freqList.InsertAfter(&freqEntry{
				freq:  nextFreq,
				items: make(map[*lfuItem]struct{}),
			}, currentFreqElement)
		}
	case nextFreqElement.Value.(*freqEntry).freq == nextFreq:
		if removable {
			c.freqList.Remove(currentFreqElement)
		}
	default:
		panic("unreachable")
	}
	nextFreqElement.Value.(*freqEntry).items[item] = struct{}{}
	item.freqElement = nextFreqElement
}

// evict 从缓存池中删除最不常用的条目.
func (c *LFUCache) evict(count int) {
	entry := c.freqList.Front()
	for i := 0; i < count; {
		if entry == nil {
			return
		} else {
			for item := range entry.Value.(*freqEntry).items {
				if i >= count {
					return
				}
				c.removeItem(item)
				i++
			}
			entry = entry.Next()
		}
	}
}

// Has 判断键是否存在.
func (c *LFUCache) Has(key interface{}) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := time.Now()
	return c.has(key, &now)
}

func (c *LFUCache) has(key interface{}, now *time.Time) bool {
	item, ok := c.items[key]
	if !ok {
		return false
	}
	return !item.IsExpired(now)
}

// Remove 从缓存池中删除一个键值对.
func (c *LFUCache) Remove(key interface{}) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.remove(key)
}

func (c *LFUCache) remove(key interface{}) bool {
	if item, ok := c.items[key]; ok {
		c.removeItem(item)
		return true
	}
	return false
}

// removeElement 从缓存池中删除一个条目.
func (c *LFUCache) removeItem(item *lfuItem) {
	entry := item.freqElement.Value.(*freqEntry)
	delete(c.items, item.key)
	delete(entry.items, item)
	if isRemovableFreqEntry(entry) {
		c.freqList.Remove(item.freqElement)
	}
	if c.evictedFunc != nil {
		c.evictedFunc(item.key, item.value)
	}
}

func (c *LFUCache) keys() []interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]interface{}, len(c.items))
	var i = 0
	for k := range c.items {
		keys[i] = k
		i++
	}
	return keys
}

// GetALL 返回所有键值对.
func (c *LFUCache) GetALL(checkExpired bool) map[interface{}]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	items := make(map[interface{}]interface{}, len(c.items))
	now := time.Now()
	for k, item := range c.items {
		if !checkExpired || c.has(k, &now) {
			items[k] = item.value
		}
	}
	return items
}

// Keys 返回一个键值对列表.
func (c *LFUCache) Keys(checkExpired bool) []interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]interface{}, 0, len(c.items))
	now := time.Now()
	for k := range c.items {
		if !checkExpired || c.has(k, &now) {
			keys = append(keys, k)
		}
	}
	return keys
}

// Len 返回缓存池中键值对的数量.
func (c *LFUCache) Len(checkExpired bool) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !checkExpired {
		return len(c.items)
	}
	var length int
	now := time.Now()
	for k := range c.items {
		if c.has(k, &now) {
			length++
		}
	}
	return length
}

// 清空缓存池.
func (c *LFUCache) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.purgeVisitorFunc != nil {
		for key, item := range c.items {
			c.purgeVisitorFunc(key, item.value)
		}
	}

	c.init()
}

// IsExpired 返回一个布尔值，判断该键值对是否过期.
func (it *lfuItem) IsExpired(now *time.Time) bool {
	if it.expiration == nil {
		return false
	}
	if now == nil {
		t := it.clock.Now()
		now = &t
	}
	return it.expiration.Before(*now)
}

func isRemovableFreqEntry(entry *freqEntry) bool {
	return entry.freq != 0 && len(entry.items) == 0
}
