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

import "github.com/zuoyebang/bitalostored/proxy/resp"

func (pc *ProxyClient) LPush(s *resp.Session, key []byte, members ...[]byte) (interface{}, error) {
	args := resp.InterfaceByteSubKeys(key, members)
	return pc.do("LPUSH", s, args...)
}

func (pc *ProxyClient) LPushX(s *resp.Session, key []byte, members ...[]byte) (interface{}, error) {
	args := resp.InterfaceByteSubKeys(key, members)
	return pc.do("LPUSHX", s, args...)
}

func (pc *ProxyClient) RPush(s *resp.Session, key []byte, members ...[]byte) (interface{}, error) {
	args := resp.InterfaceByteSubKeys(key, members)
	return pc.do("RPUSH", s, args...)
}

func (pc *ProxyClient) RPushX(s *resp.Session, key []byte, members ...[]byte) (interface{}, error) {
	args := resp.InterfaceByteSubKeys(key, members)
	return pc.do("RPUSHX", s, args...)
}

func (pc *ProxyClient) LPop(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do("LPOP", s, key)
}

func (pc *ProxyClient) RPop(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do("RPOP", s, key)
}

func (pc *ProxyClient) LRem(s *resp.Session, key []byte, count int, value interface{}) (interface{}, error) {
	return pc.do("LREM", s, key, count, value)
}

func (pc *ProxyClient) LLen(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do("LLEN", s, key)
}

func (pc *ProxyClient) LIndex(s *resp.Session, key []byte, index int) (interface{}, error) {
	return pc.do("LINDEX", s, key, index)
}

func (pc *ProxyClient) LInsert(s *resp.Session, key []byte, before string, pivot string, value string) (interface{}, error) {
	return pc.do("LINSERT", s, key, before, pivot, value)
}

func (pc *ProxyClient) LSet(s *resp.Session, key []byte, index int, value interface{}) (interface{}, error) {
	return pc.do("LSET", s, key, index, value)
}

func (pc *ProxyClient) LRange(s *resp.Session, key []byte, start int, stop int) (interface{}, error) {
	return pc.do("LRANGE", s, key, start, stop)
}

func (pc *ProxyClient) LTrim(s *resp.Session, key []byte, start int, stop int) (interface{}, error) {
	return pc.do("LTRIM", s, key, start, stop)
}

func (pc *ProxyClient) LClear(s *resp.Session, keys ...[]byte) (interface{}, error) {
	args := resp.InterfaceByte(keys)
	return pc.do(resp.LCLEAR, s, args...)
}

func (pc *ProxyClient) LExpire(s *resp.Session, key []byte, duration int64) (interface{}, error) {
	return pc.do(resp.LEXPIRE, s, key, duration)
}

func (pc *ProxyClient) LExpireAt(s *resp.Session, key []byte, when int64) (interface{}, error) {
	return pc.do(resp.LEXPIREAT, s, key, when)
}

func (pc *ProxyClient) LTtl(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.LTTL, s, key)
}

func (pc *ProxyClient) LKeyExists(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.LKEYEXISTS, s, key)
}

func (pc *ProxyClient) LPersist(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.LPERSIST, s, key)
}

func (pc *ProxyClient) LTrimBack(s *resp.Session, key []byte, size int64) (interface{}, error) {
	return pc.do(resp.LTRIMBACK, s, key, size)
}

func (pc *ProxyClient) LTrimFront(s *resp.Session, key []byte, size int64) (interface{}, error) {
	return pc.do(resp.LTRIMFRONT, s, key, size)
}
