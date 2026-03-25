package localcache

import (
	"time"
)

const (
	shardSize = 1 << 10
	minShards = 16
	maxShards = 64
)

var _ Cache = &Shards{}

type Shards struct {
	shards  int
	buckets []Cache
}

func NewShards(shards int, cb *CacheBuilder) *Shards {
	var s = &Shards{
		shards:  shards,
		buckets: make([]Cache, shards),
	}
	for i := 0; i < shards; i++ {
		s.buckets[i] = cb.Build()
	}
	return s
}

func (s *Shards) shardIndex(key interface{}) int {
	return int(hashInterface(key)) % s.shards
}

// Get implements Cache.
func (s *Shards) Get(key interface{}) (interface{}, error) {
	shardIndex := s.shardIndex(key)
	return s.buckets[shardIndex].Get(key)
}

// GetALL implements Cache.
func (s *Shards) GetALL(checkExpired bool) map[interface{}]interface{} {
	all := make(map[interface{}]interface{})
	for _, bucket := range s.buckets {
		all = mergeMaps(all, bucket.GetALL(checkExpired))
	}
	return all
}

// GetIFPresent implements Cache.
func (s *Shards) GetIFPresent(key interface{}) (interface{}, error) {
	shardIndex := s.shardIndex(key)
	return s.buckets[shardIndex].GetIFPresent(key)
}

// Has implements Cache.
func (s *Shards) Has(key interface{}) bool {
	shardIndex := s.shardIndex(key)
	return s.buckets[shardIndex].Has(key)
}

// HitCount implements Cache.
func (s *Shards) HitCount() uint64 {
	hitCount := uint64(0)
	for _, bucket := range s.buckets {
		hitCount += bucket.HitCount()
	}
	return hitCount
}

// HitRate implements Cache.
func (s *Shards) HitRate() float64 {
	hitCount := uint64(0)
	for _, bucket := range s.buckets {
		hitCount += bucket.HitCount()
	}
	return float64(hitCount) / float64(s.LookupCount())
}

// Keys implements Cache.
func (s *Shards) Keys(checkExpired bool) []interface{} {
	keys := make([]interface{}, 0)
	for _, bucket := range s.buckets {
		keys = append(keys, bucket.Keys(checkExpired)...)
	}
	return keys
}

// Len implements Cache.
func (s *Shards) Len(checkExpired bool) int {
	len := 0
	for _, bucket := range s.buckets {
		len += bucket.Len(checkExpired)
	}
	return len
}

// LookupCount implements Cache.
func (s *Shards) LookupCount() uint64 {
	lookupCount := uint64(0)
	for _, bucket := range s.buckets {
		lookupCount += bucket.LookupCount()
	}
	return lookupCount
}

// MissCount implements Cache.
func (s *Shards) MissCount() uint64 {
	missCount := uint64(0)
	for _, bucket := range s.buckets {
		missCount += bucket.MissCount()
	}
	return missCount
}

// Purge implements Cache.
func (s *Shards) Purge() {
	for _, bucket := range s.buckets {
		bucket.Purge()
	}
}

// Remove implements Cache.
func (s *Shards) Remove(key interface{}) bool {
	shardIndex := s.shardIndex(key)
	return s.buckets[shardIndex].Remove(key)
}

// Set implements Cache.
func (s *Shards) Set(key interface{}, value interface{}) error {
	shardIndex := s.shardIndex(key)
	return s.buckets[shardIndex].Set(key, value)
}

// SetWithExpire implements Cache.
func (s *Shards) SetWithExpire(key interface{}, value interface{}, expiration time.Duration) error {
	shardIndex := s.shardIndex(key)
	return s.buckets[shardIndex].SetWithExpire(key, value, expiration)
}

// get implements Cache.
func (s *Shards) get(key interface{}, onLoad bool) (interface{}, error) {
	shardIndex := s.shardIndex(key)
	return s.buckets[shardIndex].get(key, onLoad)
}

func mergeMaps(m1, m2 map[interface{}]interface{}) map[interface{}]interface{} {
	for k, v := range m2 {
		m1[k] = v
	}
	return m1
}
