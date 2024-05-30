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
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/proxy/resp"

	"github.com/gomodule/redigo/redis"
)

func (pc *ProxyClient) ZAdd(s *resp.Session, key string, maps map[string]float64) (interface{}, error) {
	args := make([]interface{}, 0, 1+len(maps)*2)
	args = append(args, key)
	for member, score := range maps {
		args = append(args, score, member)
	}
	return pc.do("ZADD", s, args...)
}

func (pc *ProxyClient) ZScore(s *resp.Session, key []byte, member []byte) (interface{}, error) {
	return pc.do("ZSCORE", s, key, member)
}

func (pc *ProxyClient) ZIncrBy(s *resp.Session, key []byte, delta float64, member []byte) (interface{}, error) {
	return pc.do("ZINCRBY", s, key, delta, member)
}

func (pc *ProxyClient) ZCard(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do("ZCARD", s, key)
}

func (pc *ProxyClient) ZCount(s *resp.Session, key, min, max string) (interface{}, error) {
	return pc.do("ZCOUNT", s, key, min, max)
}

func (pc *ProxyClient) ZLexCount(s *resp.Session, key, min, max string) (interface{}, error) {
	return pc.do("ZLEXCOUNT", s, key, min, max)
}

func (pc *ProxyClient) ZRange(s *resp.Session, key string, start int, stop int, withscores bool) (interface{}, error) {
	args := []interface{}{key, start, stop}
	if withscores {
		args = append(args, "WITHSCORES")
	}
	return pc.do("ZRANGE", s, args...)
}

func (pc *ProxyClient) ZRevRange(s *resp.Session, key string, start int, stop int, withscores bool) (interface{}, error) {
	args := []interface{}{key, start, stop}
	if withscores {
		args = append(args, "WITHSCORES")
	}
	return pc.do("ZREVRANGE", s, args...)
}

func (pc *ProxyClient) ZRangeByScore(s *resp.Session, key, min, max string, withscores, limit bool, offset int, count int) (interface{}, error) {
	args := []interface{}{key, min, max}
	if withscores {
		args = append(args, "WITHSCORES")
	}
	if limit {
		args = append(args, "LIMIT", offset, count)
	}
	return pc.do("ZRANGEBYSCORE", s, args...)
}

func (pc *ProxyClient) ZRevRangeByScore(s *resp.Session, key, max, min string, withscores, limit bool, offset int, count int) (interface{}, error) {
	args := []interface{}{key, max, min}
	if withscores {
		args = append(args, "WITHSCORES")
	}
	if limit {
		args = append(args, "LIMIT", offset, count)
	}

	return pc.do("ZREVRANGEBYSCORE", s, args...)
}

func (pc *ProxyClient) ZRank(s *resp.Session, key []byte, member []byte) (interface{}, error) {
	return pc.do("ZRANK", s, key, member)
}

func (pc *ProxyClient) ZRevRank(s *resp.Session, key []byte, member []byte) (interface{}, error) {
	return pc.do("ZREVRANK", s, key, member)
}

func (pc *ProxyClient) ZRem(s *resp.Session, key []byte, members ...[]byte) (interface{}, error) {
	args := resp.InterfaceByteSubKeys(key, members)
	return pc.do("ZREM", s, args...)
}

func (pc *ProxyClient) ZRemRangeByRank(s *resp.Session, key string, start int, stop int) (interface{}, error) {
	args := []interface{}{key, start, stop}
	return pc.do("ZREMRANGEBYRANK", s, args...)
}

func (pc *ProxyClient) ZRemRangeByScore(s *resp.Session, key, min, max string) (interface{}, error) {
	return pc.do("ZREMRANGEBYSCORE", s, key, min, max)
}

func (pc *ProxyClient) ZRemRangeByLex(s *resp.Session, key, min, max string) (interface{}, error) {
	return pc.do("ZREMRANGEBYLEX", s, key, min, max)
}

func (pc *ProxyClient) ZScan(s *resp.Session, key string, cursor []byte, pattern string, count int) ([]byte, [][]byte, error) {
	args := make([]interface{}, 0, 6)
	args = append(args, key, unsafe2.String(cursor))
	if pattern != "" {
		args = append(args, "MATCH", pattern)
	}
	if count > 0 {
		args = append(args, "COUNT", count)
	}
	values, err := redis.Values(pc.do("ZSCAN", s, args...))
	if err != nil {
		return resp.NoScanMember, nil, err
	}
	var items [][]byte
	_, err = redis.Scan(values, &cursor, &items)
	if err != nil {
		return resp.NoScanMember, nil, err
	}
	return cursor, items, nil
}

func (pc *ProxyClient) ZClear(s *resp.Session, keys ...[]byte) (interface{}, error) {
	args := resp.InterfaceByte(keys)
	return pc.do(resp.ZCLEAR, s, args...)
}

func (pc *ProxyClient) ZExpire(s *resp.Session, key []byte, duration int64) (interface{}, error) {
	return pc.do(resp.ZEXPIRE, s, key, duration)
}

func (pc *ProxyClient) ZExpireAt(s *resp.Session, key []byte, when int64) (interface{}, error) {
	return pc.do(resp.ZEXPIREAT, s, key, when)
}

func (pc *ProxyClient) ZTtl(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.ZTTL, s, key)
}

func (pc *ProxyClient) ZKeyExists(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.ZTTL, s, key)
}

func (pc *ProxyClient) ZPersist(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.ZPERSIST, s, key)
}

func (pc *ProxyClient) ZRangeByLex(s *resp.Session, args ...interface{}) (interface{}, error) {
	return pc.do(resp.ZRANGEBYLEX, s, args...)
}
