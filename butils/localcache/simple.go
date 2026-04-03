package localcache

import (
	"time"
)

// SimpleCache 没有明确的淘汰策略，依赖于键值对map的顺序.
type SimpleCache struct {
	baseCache
	items map[interface{}]*simpleItem
}

func newSimpleCache(cb *CacheBuilder) *SimpleCache {
	c := &SimpleCache{}
	buildCache(&c.baseCache, cb)

	c.init()
	c.loadGroup.cache = c
	return c
}

func (c *SimpleCache) init() {
	if c.size <= 0 {
		c.items = make(map[interface{}]*simpleItem)
	} else {
		c.items = make(map[interface{}]*simpleItem, c.size)
	}
}

// 设置一个新键值对
func (c *SimpleCache) Set(key, value interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.set(key, value)
	return err
}

// 设置一个新键值对，并设置过期时间
func (c *SimpleCache) SetWithExpire(key, value interface{}, expiration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, err := c.set(key, value)
	if err != nil {
		return err
	}

	t := c.clock.Now().Add(expiration)
	item.(*simpleItem).expiration = &t
	return nil
}

func (c *SimpleCache) set(key, value interface{}) (interface{}, error) {
	var err error
	if c.serializeFunc != nil {
		value, err = c.serializeFunc(key, value)
		if err != nil {
			return nil, err
		}
	}

	// Check for existing item
	item, ok := c.items[key]
	if ok {
		item.value = value
	} else {
		// Verify size not exceeded
		if (len(c.items) >= c.size) && c.size > 0 {
			c.evict(1)
		}
		item = &simpleItem{
			clock: c.clock,
			value: value,
		}
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
func (c *SimpleCache) Get(key interface{}) (interface{}, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, true)
	}
	return v, err
}

// 从缓存池中获取一个值，如果存在则返回.
// 如果键不存在，则返回KeyNotFoundError.
// 如果缓存对象有LoaderFunc，则发送请求刷新指定键的值.
func (c *SimpleCache) GetIFPresent(key interface{}) (interface{}, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, false)
	}
	return v, nil
}

func (c *SimpleCache) get(key interface{}, onLoad bool) (interface{}, error) {
	v, err := c.getValue(key, onLoad)
	if err != nil {
		return nil, err
	}
	if c.deserializeFunc != nil {
		return c.deserializeFunc(key, v)
	}
	return v, nil
}

func (c *SimpleCache) getValue(key interface{}, onLoad bool) (interface{}, error) {
	c.mu.Lock()
	item, ok := c.items[key]
	if ok {
		if !item.IsExpired(nil) {
			v := item.value
			c.mu.Unlock()
			if !onLoad {
				c.stats.IncrHitCount()
			}
			return v, nil
		}
		c.remove(key)
	}
	c.mu.Unlock()
	if !onLoad {
		c.stats.IncrMissCount()
	}
	return nil, KeyNotFoundError
}

func (c *SimpleCache) getWithLoader(key interface{}, isWait bool) (interface{}, error) {
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
			item.(*simpleItem).expiration = &t
		}
		return v, nil
	}, isWait)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (c *SimpleCache) evict(count int) {
	now := c.clock.Now()
	current := 0
	for key, item := range c.items {
		if current >= count {
			return
		}
		if item.expiration == nil || now.After(*item.expiration) {
			defer c.remove(key)
			current++
		}
	}
}

// Has 判断键是否存在.
func (c *SimpleCache) Has(key interface{}) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := time.Now()
	return c.has(key, &now)
}

func (c *SimpleCache) has(key interface{}, now *time.Time) bool {
	item, ok := c.items[key]
	if !ok {
		return false
	}
	return !item.IsExpired(now)
}

// Remove 从缓存池中删除一个键值对.
func (c *SimpleCache) Remove(key interface{}) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.remove(key)
}

func (c *SimpleCache) remove(key interface{}) bool {
	item, ok := c.items[key]
	if ok {
		delete(c.items, key)
		if c.evictedFunc != nil {
			c.evictedFunc(key, item.value)
		}
		return true
	}
	return false
}

// 返回一个键值对列表.
func (c *SimpleCache) keys() []interface{} {
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

// 返回所有键值对.
func (c *SimpleCache) GetALL(checkExpired bool) map[interface{}]interface{} {
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
func (c *SimpleCache) Keys(checkExpired bool) []interface{} {
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
func (c *SimpleCache) Len(checkExpired bool) int {
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
func (c *SimpleCache) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.purgeVisitorFunc != nil {
		for key, item := range c.items {
			c.purgeVisitorFunc(key, item.value)
		}
	}

	c.init()
}

type simpleItem struct {
	clock      Clock
	value      interface{}
	expiration *time.Time
}

// IsExpired 返回一个布尔值，判断该键值对是否过期.
func (si *simpleItem) IsExpired(now *time.Time) bool {
	if si.expiration == nil {
		return false
	}
	if now == nil {
		t := si.clock.Now()
		now = &t
	}
	return si.expiration.Before(*now)
}
