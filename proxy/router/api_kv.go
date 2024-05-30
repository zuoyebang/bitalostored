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
	"errors"
	"time"

	"github.com/zuoyebang/bitalostored/proxy/internal/log"
	"github.com/zuoyebang/bitalostored/proxy/resp"

	"github.com/gomodule/redigo/redis"
)

const (
	DefaultLocalCacheExpireTime = 120 * time.Second
)

type HitStatus int

const (
	NotUseCacheStatus HitStatus = 0
	HitCacheStatus    HitStatus = 1
	NotHitCacheStatus HitStatus = 2
)

func (pc *ProxyClient) Get(s *resp.Session, key string) (interface{}, error) {
	var checkCache bool
	if s != nil {
		checkCache = pc.checkKeyIsProxyCache(key)
		if checkCache {
			if res, find := pc.router.localCache.Get(key); find {
				return res.([]byte), nil
			}
		}
	}
	data, err := pc.do(resp.GET, s, key)
	if s != nil {
		return data, err
	}
	res, err := redis.Bytes(data, err)
	if checkCache && err != nil && res != nil {
		pc.router.localCache.Set(key, res, DefaultLocalCacheExpireTime)
	}
	return res, err
}

func (pc *ProxyClient) GetSet(s *resp.Session, key string, value string) (interface{}, error) {
	checkCache := pc.checkKeyIsProxyCache(key)
	if checkCache {
		pc.router.localCache.Delete(key)
	}

	return pc.do(resp.GETSET, s, key, value)
}

func (pc *ProxyClient) MGet(s *resp.Session, keys [][]byte) (interface{}, error) {
	if len(keys) <= 0 {
		return nil, nil
	}

	args := resp.InterfaceByte(keys)
	return pc.do("MGET", s, args...)
}

func (pc *ProxyClient) MSet(s *resp.Session, values ...string) (interface{}, error) {
	if len(values)%2 != 0 {
		return nil, errors.New("mset missing value")
	}

	args := resp.InterfaceString(values)
	if _, err := pc.do("MSET", s, args...); err != nil {
		log.Warnf("USE_ONLY_STORED MSet err:%s", err.Error())
		return nil, err
	}
	if s == nil {
		pc.mSetToGocache(values...)
	}
	return nil, nil
}

func (pc *ProxyClient) Set(s *resp.Session, key string, value string, exType resp.ExpireType, expire int64) (interface{}, error) {
	checkCache := pc.checkKeyIsProxyCache(key)
	setCacheFunc := func(checkCache bool) {
		if checkCache {
			pc.router.localCache.Delete(key)
		}
	}
	var err error
	if exType == resp.NoType {
		if _, err = pc.do(resp.SET, s, key, value); err != nil {
			return nil, err
		}
	} else {
		if _, err = pc.do(resp.SET, s, key, value, exType, expire); err != nil {
			return nil, err
		}
	}
	setCacheFunc(checkCache)
	return nil, err
}

func (pc *ProxyClient) SetNx(s *resp.Session, key []byte, value []byte) (interface{}, error) {
	return pc.do(resp.SETNX, s, key, value)
}

func (pc *ProxyClient) SetNxByEX(s *resp.Session, key []byte, value []byte, expire uint64) (interface{}, error) {
	return pc.do(resp.SET, s, key, value, string(resp.EX), expire, string(resp.NX))
}

func (pc *ProxyClient) SetNxByPX(s *resp.Session, key []byte, value []byte, expire uint64) (interface{}, error) {
	return pc.do(resp.SET, s, key, value, string(resp.PX), expire, string(resp.NX))
}

func (pc *ProxyClient) Incr(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do("INCR", s, key)
}

func (pc *ProxyClient) IncrBy(s *resp.Session, key []byte, value int64) (interface{}, error) {
	return pc.do("INCRBY", s, key, value)
}

func (pc *ProxyClient) IncrByFloat(s *resp.Session, key []byte, value interface{}) (interface{}, error) {
	return pc.do("INCRBYFLOAT", s, key, value)
}

func (pc *ProxyClient) Decr(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do("DECR", s, key)
}

func (pc *ProxyClient) DecrBy(s *resp.Session, key []byte, value int64) (interface{}, error) {
	return pc.do("DECRBY", s, key, value)
}

func (pc *ProxyClient) mSetToGocache(values ...string) error {
	if len(values)%2 != 0 {
		return errors.New("missing value")
	}

	useCache := make([]string, 0, len(values))

	for i := 0; i < len(values); i = i + 2 {
		key := values[i]
		value := values[i+1]
		if pc.checkKeyIsProxyCache(key) {
			useCache = append(useCache, key, value)
		}
	}

	if len(useCache) <= 0 {
		return nil
	}

	return pc.router.localCache.MSet(DefaultLocalCacheExpireTime, useCache...)
}

func (pc *ProxyClient) mGetFromGocache(res [][]byte, keys ...string) ([]string, []HitStatus) {
	missCacheKey := make([]string, 0, len(keys))
	resCacheIndexHitStatus := make([]HitStatus, len(keys), len(keys))
	for i := range keys {
		resCacheIndexHitStatus[i] = NotUseCacheStatus
	}

	for i, key := range keys {
		if pc.checkKeyIsProxyCache(key) {
			data, find := pc.router.localCache.Get(key)
			if find {
				res[i] = data.([]byte)
				resCacheIndexHitStatus[i] = HitCacheStatus
			} else {
				res[i] = nil
				missCacheKey = append(missCacheKey, key)
				resCacheIndexHitStatus[i] = NotHitCacheStatus
			}
		} else {
			missCacheKey = append(missCacheKey, key)
		}
	}
	return missCacheKey, resCacheIndexHitStatus
}

func (pc *ProxyClient) mGetCacheReSave(keys []string, res [][]byte, missCacheIndex []HitStatus) {
	resLen := len(res)
	for i, hitstatus := range missCacheIndex {
		if hitstatus == NotHitCacheStatus {
			if resLen > i && res[i] != nil {
				pc.router.localCache.Set(keys[i], res[i], DefaultLocalCacheExpireTime)
			}
		}
	}
}

func (pc *ProxyClient) StrLen(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.STRLEN, s, key)
}

func (pc *ProxyClient) GetRange(s *resp.Session, key []byte, start, end int) (interface{}, error) {
	return pc.do(resp.GETRANGE, s, key, start, end)
}

func (pc *ProxyClient) SetRange(s *resp.Session, key string, offset int, value string) (interface{}, error) {
	checkCache := pc.checkKeyIsProxyCache(key)
	if checkCache {
		pc.router.localCache.Delete(key)
	}
	return pc.do(resp.SETRANGE, s, key, offset, value)
}

func (pc *ProxyClient) Append(s *resp.Session, key string, value string) (interface{}, error) {
	checkCache := pc.checkKeyIsProxyCache(key)
	if checkCache {
		pc.router.localCache.Delete(key)
	}
	return pc.do(resp.APPEND, s, key, value)
}

func (pc *ProxyClient) KExpireAt(s *resp.Session, key []byte, when int64) (interface{}, error) {
	return pc.do(resp.KEXPIREAT, s, key, when)
}

func (pc *ProxyClient) KExpire(s *resp.Session, key string, duration int64) (interface{}, error) {
	checkCache := pc.checkKeyIsProxyCache(key)
	if checkCache {
		if value, find := pc.router.localCache.Get(key); find {
			pc.router.localCache.Set(key, value, time.Duration(duration)*time.Second)
		}
	}
	return pc.do(resp.KEXPIRE, s, key, duration)
}

func (pc *ProxyClient) KTtl(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.KTTL, s, key)
}

func (pc *ProxyClient) KExists(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.KEXISTS, s, key)
}

func (pc *ProxyClient) KDel(s *resp.Session, keys ...string) (interface{}, error) {
	args := resp.InterfaceString(keys)
	return pc.do(resp.KDEL, s, args...)
}

func (pc *ProxyClient) KPersist(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.KPERSIST, s, key)
}

func (pc *ProxyClient) GetBit(s *resp.Session, key []byte, offset int) (interface{}, error) {
	return pc.do(resp.GETBIT, s, key, offset)
}

func (pc *ProxyClient) SetBit(s *resp.Session, key []byte, offset, value int) (interface{}, error) {
	return pc.do(resp.SETBIT, s, key, offset, value)
}

func (pc *ProxyClient) BitCount(s *resp.Session, args ...interface{}) (interface{}, error) {
	return pc.do(resp.BITCOUNT, s, args...)
}

func (pc *ProxyClient) BitPos(s *resp.Session, key []byte, bit, start, end int) (interface{}, error) {
	return pc.do(resp.BITPOS, s, key, bit, start, end)
}
