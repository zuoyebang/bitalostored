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
)

func (pc *ProxyClient) GeoAdd(s *resp.Session, args ...interface{}) (interface{}, error) {
	return pc.do(resp.GEOADD, s, args...)
}

func (pc *ProxyClient) GeoDist(s *resp.Session, args [][]byte) (interface{}, error) {
	var params []interface{}
	for _, data := range args {
		params = append(params, unsafe2.String(data))
	}
	return pc.do(resp.GEODIST, s, params...)
}

func (pc *ProxyClient) GeoRadius(s *resp.Session, args ...interface{}) (interface{}, error) {
	return pc.do(resp.GEORADIUS, s, args...)
}

func (pc *ProxyClient) GeoPos(s *resp.Session, args [][]byte) (interface{}, error) {
	var params []interface{}
	for _, data := range args {
		params = append(params, unsafe2.String(data))
	}
	return pc.do(resp.GEOPOS, s, params...)
}

func (pc *ProxyClient) GeoHash(s *resp.Session, args [][]byte) (interface{}, error) {
	var params []interface{}
	for _, data := range args {
		params = append(params, unsafe2.String(data))
	}
	return pc.do(resp.GEOHASH, s, params...)
}

func (pc *ProxyClient) GeoRadiusByMember(s *resp.Session, args ...interface{}) (interface{}, error) {
	return pc.do(resp.GEORADIUSBYMEMBER, s, args...)
}
