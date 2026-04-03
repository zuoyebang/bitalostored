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

	"github.com/zuoyebang/bitalostored/proxy/internal/log"
	"github.com/zuoyebang/bitalostored/proxy/resp"

	"github.com/gomodule/redigo/redis"
)

func (pc *ProxyClient) Get(s *resp.Session, key string) (interface{}, error) {
	data, err := pc.do(resp.GET, s, key)
	if s != nil {
		return data, err
	}
	res, err := redis.Bytes(data, err)
	return res, err
}

func (pc *ProxyClient) GetSet(s *resp.Session, key string, value string) (interface{}, error) {
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
	return nil, nil
}

func (pc *ProxyClient) Set(s *resp.Session, key string, value string, exType resp.ExpireType, expire int64) (interface{}, error) {
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

func (pc *ProxyClient) StrLen(s *resp.Session, key []byte) (interface{}, error) {
	return pc.do(resp.STRLEN, s, key)
}

func (pc *ProxyClient) GetRange(s *resp.Session, key []byte, start, end int) (interface{}, error) {
	return pc.do(resp.GETRANGE, s, key, start, end)
}

func (pc *ProxyClient) SetRange(s *resp.Session, key string, offset int, value string) (interface{}, error) {
	return pc.do(resp.SETRANGE, s, key, offset, value)
}

func (pc *ProxyClient) Append(s *resp.Session, key string, value string) (interface{}, error) {
	return pc.do(resp.APPEND, s, key, value)
}

func (pc *ProxyClient) KExpireAt(s *resp.Session, key []byte, when int64) (interface{}, error) {
	return pc.do(resp.KEXPIREAT, s, key, when)
}

func (pc *ProxyClient) KExpire(s *resp.Session, key string, duration int64) (interface{}, error) {
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

func (pc *ProxyClient) GetBit64(s *resp.Session, key []byte, offset int) (interface{}, error) {
	return pc.do(resp.GETBIT64, s, key, offset)
}

func (pc *ProxyClient) SetBit64(s *resp.Session, key []byte, offset, value int) (interface{}, error) {
	return pc.do(resp.SETBIT64, s, key, offset, value)
}

func (pc *ProxyClient) BitCount64(s *resp.Session, args ...interface{}) (interface{}, error) {
	return pc.do(resp.BITCOUNT64, s, args...)
}

func (pc *ProxyClient) BitPos64(s *resp.Session, key []byte, bit, start, end int) (interface{}, error) {
	return pc.do(resp.BITPOS64, s, key, bit, start, end)
}
