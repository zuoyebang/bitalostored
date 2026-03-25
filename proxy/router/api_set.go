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
	"github.com/zuoyebang/bitalostored/proxy/resp"
)

func (pc *ProxyClient) SAdd(s *resp.Session, key []byte, members ...[]byte) (interface{}, error) {
	args := resp.InterfaceByteSubKeys(key, members)
	return pc.do("SADD", s, args...)
}

func (pc *ProxyClient) SRandMemberCommandWithCount(s *resp.Session, key []byte, count int) (interface{}, error) {
	return pc.do(resp.SRANDMEMBER, s, key, count)
}

func (pc *ProxyClient) SRandMemberCommandWithoutCount(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.SRANDMEMBER, s, key)
}

func (pc *ProxyClient) SIsMember(s *resp.Session, key []byte, member []byte) (interface{}, error) {
	return pc.do("SISMEMBER", s, key, member)
}

func (pc *ProxyClient) SMembers(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do("SMembers", s, key)
}

func (pc *ProxyClient) SRem(s *resp.Session, key []byte, members ...[]byte) (interface{}, error) {
	args := resp.InterfaceByteSubKeys(key, members)
	return pc.do("SREM", s, args...)
}

func (pc *ProxyClient) SPop(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.SPOP, s, key)
}

func (pc *ProxyClient) SPopByCount(s *resp.Session, key []byte, count int64) (interface{}, error) {
	return pc.do(resp.SPOP, s, key, count)
}

func (pc *ProxyClient) SCard(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do("SCARD", s, key)
}

func (pc *ProxyClient) SClear(s *resp.Session, keys ...[]byte) (interface{}, error) {
	args := resp.InterfaceByte(keys)
	return pc.do(resp.SCLEAR, s, args...)
}

func (pc *ProxyClient) SExpire(s *resp.Session, key []byte, duration int64) (interface{}, error) {
	return pc.do(resp.SEXPIRE, s, key, duration)
}

func (pc *ProxyClient) SExpireAt(s *resp.Session, key []byte, when int64) (interface{}, error) {
	return pc.do(resp.SEXPIREAT, s, key, when)
}

func (pc *ProxyClient) STtl(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.STTL, s, key)
}

func (pc *ProxyClient) SKeyExists(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.SKEYEXISTS, s, key)
}

func (pc *ProxyClient) SPersist(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.SPERSIST, s, key)
}
