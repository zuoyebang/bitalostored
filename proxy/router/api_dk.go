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
	"github.com/zuoyebang/bitalostored/proxy/internal/log"
	"github.com/zuoyebang/bitalostored/proxy/resp"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/gomodule/redigo/redis"
)

func (pc *ProxyClient) DkCreate(key []byte, shardNum uint32, dt string) (interface{}, error) {
	return pc.dkDo(resp.DK_CREATE, key, shardNum, dt)
}

func (pc *ProxyClient) DkInfo(key []byte) (interface{}, error) {
	return pc.dkDo(resp.DK_INFO, key)
}

func (pc *ProxyClient) DkCreateShard(dt string, args ...interface{}) (interface{}, error) {
	return pc.dkDo(resp.DK_CREATESHARD, dt, args)
}

func (pc *ProxyClient) DkIncrBySize(key []byte, value int64) (interface{}, error) {
	return pc.dkDo(resp.DK_INCRBYSIZE, key, value)
}

func (pc *ProxyClient) GetDkShardNum(key string) (uint32, error) {
	sn, _ := pc.GetSimpleCache(key)
	if sn != nil {
		return sn.(uint32), nil
	}
	log.Infof("dk local cache empty, get info key:%s", key)
	_, shardNum, _, _, err := pc.GetDkInfo(key)
	if err != nil {
		return 0, err
	}
	if shardNum > 0 {
		_ = pc.SetDkShardNum(key, shardNum)
	}
	return shardNum, nil
}

func (pc *ProxyClient) SetDkShardNum(key string, shardNum uint32) error {
	return pc.SetSimpleCacheWithExpire(key, shardNum, DkExpireTime)
}

// dataType(uint8) shardNum(uint32) size(uint64) timestamp(int64)
func (pc *ProxyClient) GetDkInfo(key string) (uint8, uint32, int64, int64, error) {
	infoRes, err := pc.dkDo(resp.DK_INFO, key)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	resTmp, err := redis.ByteSlices(infoRes, err)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	dt := resTmp[0][0] - '0'
	sn, _ := extend.ParseUint32(unsafe2.String(resTmp[1]))
	size, _ := extend.ParseInt64(unsafe2.String(resTmp[2]))
	ts, _ := extend.ParseInt64(unsafe2.String(resTmp[3]))
	return dt, sn, size, ts, nil
}
