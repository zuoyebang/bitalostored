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

func (pc *ProxyClient) DkHGet(key, field []byte) (interface{}, error) {
	return pc.dkDo(resp.DK_HGET, key, field)
}

func (pc *ProxyClient) DkHMGet(shardNum uint32, args ...[]byte) (interface{}, error) {
	return pc.dkDo(resp.DK_HMGET, shardNum, args)
}

func (pc *ProxyClient) DkHSet(key []byte, shardNum uint32, fvmap interface{}) (interface{}, error) {
	return pc.dkDo(resp.DK_HSET, key, shardNum, fvmap)
}

func (pc *ProxyClient) DkHIncrBy(key string, field []byte, value int64) (interface{}, error) {
	return pc.dkDo(resp.HINCRBY, key, field, value)
}

func (pc *ProxyClient) DkHExists(key string, field []byte) (interface{}, error) {
	return pc.dkDo(resp.HEXISTS, key, field)
}

func (pc *ProxyClient) DkHDel(shardNum uint32, args ...[]byte) (interface{}, error) {
	return pc.dkDo(resp.DK_HDEL, shardNum, args)
}
