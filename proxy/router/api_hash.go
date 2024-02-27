// Copyright 2019 The Bitalostored author and other contributors.
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
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/proxy/resp"

	"github.com/gomodule/redigo/redis"
)

func (pc *ProxyClient) HSet(s *resp.Session, key string, field string, val string) (interface{}, error) {
	return pc.do("HSET", s, key, field, val)
}

func (pc *ProxyClient) HSetWithRes(s *resp.Session, key []byte, field []byte, val []byte) (interface{}, error) {
	return pc.do("HSET", s, key, field, val)
}

func (pc *ProxyClient) HGet(s *resp.Session, key []byte, field []byte) (interface{}, error) {
	return pc.do("HGET", s, key, field)
}

func (pc *ProxyClient) HMGet(s *resp.Session, key []byte, fields ...[]byte) (interface{}, error) {
	args := resp.InterfaceByteSubKeys(key, fields)
	return pc.do("HMGET", s, args...)
}

func (pc *ProxyClient) HMSet(s *resp.Session, key string, fvmap map[string]interface{}) (interface{}, error) {
	args := resp.PackArgs(key, fvmap)
	return pc.do("HMSET", s, args...)
}

func (pc *ProxyClient) HKeys(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do("HKEYS", s, key)
}

func (pc *ProxyClient) HGetAll(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do("HGETALL", s, key)
}

func (pc *ProxyClient) HLen(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do("HLEN", s, key)
}

func (pc *ProxyClient) HVals(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do("HVALS", s, key)
}

func (pc *ProxyClient) HIncrBy(s *resp.Session, key []byte, field []byte, value int64) (interface{}, error) {
	return pc.do("HINCRBY", s, key, field, value)
}

func (pc *ProxyClient) HExists(s *resp.Session, key []byte, field []byte) (interface{}, error) {
	return pc.do("HEXISTS", s, key, field)
}

func (pc *ProxyClient) HDel(s *resp.Session, key []byte, fields ...[]byte) (interface{}, error) {
	args := resp.InterfaceByteSubKeys(key, fields)
	return pc.do("HDEL", s, args...)
}

func (pc *ProxyClient) HScan(s *resp.Session, key []byte, cursor []byte, pattern string, count int) ([]byte, [][]byte, error) {
	args := make([]interface{}, 0, 6)
	args = append(args, key, unsafe2.String(cursor))
	if pattern != "" {
		args = append(args, "MATCH", pattern)
	}
	if count > 0 {
		args = append(args, "COUNT", count)
	}

	values, err := redis.Values(pc.do("HSCAN", s, args...))
	if err != nil {
		return resp.NoScanMember, nil, err
	}

	var items [][]byte
	if _, err = redis.Scan(values, &cursor, &items); err != nil {
		return resp.NoScanMember, nil, err
	}

	return cursor, items, nil
}

func (pc *ProxyClient) HClear(s *resp.Session, keys ...[]byte) (interface{}, error) {
	args := resp.InterfaceByte(keys)
	return pc.do(resp.HCLEAR, s, args...)
}

func (pc *ProxyClient) HExpire(s *resp.Session, key []byte, duration int64) (interface{}, error) {
	return pc.do(resp.HEXPIRE, s, key, duration)
}

func (pc *ProxyClient) HExpireAt(s *resp.Session, key []byte, when int64) (interface{}, error) {
	return pc.do(resp.HEXPIREAT, s, key, when)
}

func (pc *ProxyClient) HTtl(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.HTTL, s, key)
}

func (pc *ProxyClient) HKeyExists(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.HKEYEXISTS, s, key)
}

func (pc *ProxyClient) HPersist(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.HPERSIST, s, key)
}
