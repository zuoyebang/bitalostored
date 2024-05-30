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

package router

import (
	"time"

	"github.com/zuoyebang/bitalostored/proxy/resp"
)

func (pc *ProxyClient) Expire(s *resp.Session, key string, duration int64) (interface{}, error) {
	checkCache := pc.checkKeyIsProxyCache(key)
	if checkCache {
		if value, find := pc.router.localCache.Get(key); find {
			pc.router.localCache.Set(key, value, time.Duration(duration)*time.Second)
		}
	}
	return pc.do(resp.EXPIRE, s, key, duration)
}

func (pc *ProxyClient) PExpire(s *resp.Session, key string, duration int64) (interface{}, error) {
	checkCache := pc.checkKeyIsProxyCache(key)
	if checkCache {
		if value, find := pc.router.localCache.Get(key); find {
			pc.router.localCache.Set(key, value, time.Duration(duration)*time.Millisecond)
		}
	}
	return pc.do(resp.PEXPIRE, s, key, duration)
}

func (pc *ProxyClient) ExpireAt(s *resp.Session, expireType string, key []byte, when int64) (interface{}, error) {
	return pc.do(expireType, s, key, when)
}

func (pc *ProxyClient) Persist(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.PERSIST, s, key)
}

func (pc *ProxyClient) Exists(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.EXISTS, s, key)
}

func (pc *ProxyClient) Del(s *resp.Session, keys ...interface{}) (interface{}, error) {
	checkCache, needCacheKey := pc.checkKeysSaveCache(keys...)
	if checkCache && len(needCacheKey) > 0 {
		pc.router.localCache.Delete(needCacheKey...)
	}

	return pc.do(resp.DEL, s, keys...)
}

func (pc *ProxyClient) TTL(ttlType string, s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(ttlType, s, key)
}

func (pc *ProxyClient) Type(key []byte, s *resp.Session) (interface{}, error) {
	return pc.do(resp.TYPE, s, key)
}
