package router

import "time"

func (pc *ProxyClient) GetSimpleCache(key interface{}) (interface{}, error) {
	return pc.router.localCache.Get(key)
}

func (pc *ProxyClient) SetSimpleCacheWithExpire(key, value interface{}, duration time.Duration) error {
	return pc.router.localCache.SetWithExpire(key, value, duration)
}

func (pc *ProxyClient) RemoveSimpleCache(key interface{}) bool {
	return pc.router.localCache.Remove(key)
}
