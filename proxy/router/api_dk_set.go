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

func (pc *ProxyClient) DkSAdd(shardNum uint32, args ...[]byte) (interface{}, error) {
	return pc.dkDo(resp.DK_SADD, shardNum, args)
}

func (pc *ProxyClient) DkSIsMember(key string, member []byte) (interface{}, error) {
	return pc.dkDo(resp.SISMEMBER, key, member)
}

func (pc *ProxyClient) DkSRem(shardNum uint32, args ...[]byte) (interface{}, error) {
	return pc.dkDo(resp.DK_SREM, shardNum, args)
}

func (pc *ProxyClient) DkSPop(key []byte, count int64) (interface{}, error) {
	return pc.dkDo(resp.DK_SPOP, key, count)
}
