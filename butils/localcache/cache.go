package localcache

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

const (
	TYPE_SIMPLE = "simple"
	TYPE_LRU    = "lru"
	TYPE_LFU    = "lfu"
	TYPE_ARC    = "arc"
)

var KeyNotFoundError = errors.New("Key not found.")

type Cache interface {
	// Set 设置或更新数据.
	Set(key, value interface{}) error
	// SetWithExpire 设置数据并设置过期时间.
	SetWithExpire(key, value interface{}, expiration time.Duration) error
	// Get 获取数据.
	// 如果数据不存在，并且缓存有LoaderFunc，则调用LoaderFunc函数并插入数据到缓存中。
	// 如果数据不存在，并且缓存没有LoaderFunc，则返回KeyNotFoundError。
	Get(key interface{}) (interface{}, error)
	// GetIFPresent 获取数据.
	// 如果数据存在，则返回数据。
	// 如果数据不存在，则返回KeyNotFoundError。
	GetIFPresent(key interface{}) (interface{}, error)
	// GetAll 获取所有数据，避免使用，性能较差.
	GetALL(checkExpired bool) map[interface{}]interface{}
	get(key interface{}, onLoad bool) (interface{}, error)
	// Remove 删除数据.
	// 如果数据存在，则删除数据。
	// 如果数据不存在，则返回false。
	Remove(key interface{}) bool
	// Purge 清除所有数据，慎用.
	Purge()
	// Keys 获取所有Key，避免使用，性能较差.
	Keys(checkExpired bool) []interface{}
	// Len 获取数据数量.
	Len(checkExpired bool) int
	// Has 判断数据是否存在.
	Has(key interface{}) bool

	statsAccessor
}

type baseCache struct {
	clock            Clock
	size             int
	loaderExpireFunc LoaderExpireFunc
	evictedFunc      EvictedFunc
	purgeVisitorFunc PurgeVisitorFunc
	addedFunc        AddedFunc
	deserializeFunc  DeserializeFunc
	serializeFunc    SerializeFunc
	expiration       *time.Duration
	mu               sync.RWMutex
	loadGroup        Group
	*stats
}

type (
	LoaderFunc       func(interface{}) (interface{}, error)
	LoaderExpireFunc func(interface{}) (interface{}, *time.Duration, error)
	EvictedFunc      func(interface{}, interface{})
	PurgeVisitorFunc func(interface{}, interface{})
	AddedFunc        func(interface{}, interface{})
	DeserializeFunc  func(interface{}, interface{}) (interface{}, error)
	SerializeFunc    func(interface{}, interface{}) (interface{}, error)
)

type CacheBuilder struct {
	clock            Clock
	tp               string
	size             int
	shards           int
	loaderExpireFunc LoaderExpireFunc
	evictedFunc      EvictedFunc
	purgeVisitorFunc PurgeVisitorFunc
	addedFunc        AddedFunc
	expiration       *time.Duration
	deserializeFunc  DeserializeFunc
	serializeFunc    SerializeFunc
}

func New(size int) *CacheBuilder {
	return &CacheBuilder{
		clock: NewRealClock(),
		tp:    TYPE_SIMPLE,
		size:  size,
	}
}

func (cb *CacheBuilder) Clock(clock Clock) *CacheBuilder {
	cb.clock = clock
	return cb
}

// 设置一个加载函数.
// loaderFunc: 如果缓存数据过期，则调用此函数创建一个新值.
func (cb *CacheBuilder) LoaderFunc(loaderFunc LoaderFunc) *CacheBuilder {
	cb.loaderExpireFunc = func(k interface{}) (interface{}, *time.Duration, error) {
		v, err := loaderFunc(k)
		return v, nil, err
	}
	return cb
}

// 设置一个带有过期时间的加载函数.
// loaderExpireFunc: 如果缓存数据过期，则调用此函数创建一个新值.
// 如果loaderExpireFunc返回nil，则数据不会过期.
func (cb *CacheBuilder) LoaderExpireFunc(loaderExpireFunc LoaderExpireFunc) *CacheBuilder {
	cb.loaderExpireFunc = loaderExpireFunc
	return cb
}

func (cb *CacheBuilder) EvictType(tp string) *CacheBuilder {
	cb.tp = tp
	return cb
}

func (cb *CacheBuilder) Simple() *CacheBuilder {
	return cb.EvictType(TYPE_SIMPLE)
}

func (cb *CacheBuilder) LRU() *CacheBuilder {
	return cb.EvictType(TYPE_LRU)
}

func (cb *CacheBuilder) LFU() *CacheBuilder {
	return cb.EvictType(TYPE_LFU)
}

func (cb *CacheBuilder) ARC() *CacheBuilder {
	return cb.EvictType(TYPE_ARC)
}

func (cb *CacheBuilder) NoShards() *CacheBuilder {
	cb.shards = 1
	return cb
}

func (cb *CacheBuilder) EvictedFunc(evictedFunc EvictedFunc) *CacheBuilder {
	cb.evictedFunc = evictedFunc
	return cb
}

func (cb *CacheBuilder) PurgeVisitorFunc(purgeVisitorFunc PurgeVisitorFunc) *CacheBuilder {
	cb.purgeVisitorFunc = purgeVisitorFunc
	return cb
}

func (cb *CacheBuilder) AddedFunc(addedFunc AddedFunc) *CacheBuilder {
	cb.addedFunc = addedFunc
	return cb
}

func (cb *CacheBuilder) DeserializeFunc(deserializeFunc DeserializeFunc) *CacheBuilder {
	cb.deserializeFunc = deserializeFunc
	return cb
}

func (cb *CacheBuilder) SerializeFunc(serializeFunc SerializeFunc) *CacheBuilder {
	cb.serializeFunc = serializeFunc
	return cb
}

func (cb *CacheBuilder) Expiration(expiration time.Duration) *CacheBuilder {
	cb.expiration = &expiration
	return cb
}

func (cb *CacheBuilder) Build() Cache {
	if cb.size <= 0 && cb.tp != TYPE_SIMPLE {
		panic("gcache: Cache size <= 0")
	}

	if cb.shards == 0 {
		shards := int(math.Ceil(float64(cb.size) / float64(shardSize)))
		if shards > maxShards {
			shards = maxShards
		}
		if shards < minShards {
			shards = minShards
		}
		cb.size = int(math.Ceil(float64(cb.size) / float64(shards)))
		cb.shards = shards
		return NewShards(shards, cb)
	}

	return cb.build()
}

func (cb *CacheBuilder) build() Cache {
	switch cb.tp {
	case TYPE_SIMPLE:
		return newSimpleCache(cb)
	case TYPE_LRU:
		return newLRUCache(cb)
	case TYPE_LFU:
		return newLFUCache(cb)
	case TYPE_ARC:
		return newARC(cb)
	default:
		panic("gcache: Unknown type " + cb.tp)
	}
}

func buildCache(c *baseCache, cb *CacheBuilder) {
	c.clock = cb.clock
	c.size = cb.size
	c.loaderExpireFunc = cb.loaderExpireFunc
	c.expiration = cb.expiration
	c.addedFunc = cb.addedFunc
	c.deserializeFunc = cb.deserializeFunc
	c.serializeFunc = cb.serializeFunc
	c.evictedFunc = cb.evictedFunc
	c.purgeVisitorFunc = cb.purgeVisitorFunc
	c.stats = &stats{}
}

// 使用指定键加载一个新值.
func (c *baseCache) load(key interface{}, cb func(interface{}, *time.Duration, error) (interface{}, error), isWait bool) (interface{}, bool, error) {
	v, called, err := c.loadGroup.Do(key, func() (v interface{}, e error) {
		defer func() {
			if r := recover(); r != nil {
				e = fmt.Errorf("Loader panics: %v", r)
			}
		}()
		return cb(c.loaderExpireFunc(key))
	}, isWait)
	if err != nil {
		return nil, called, err
	}
	return v, called, nil
}
