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

package respcmd

import (
	"bytes"

	"github.com/zuoyebang/bitalostored/proxy/internal/utils"
	"github.com/zuoyebang/bitalostored/proxy/resp"
	"github.com/zuoyebang/bitalostored/proxy/router"

	"github.com/gomodule/redigo/redis"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
)

func init() {
	resp.Register(resp.DK_EXISTS, DkExistsCommand)
	resp.Register(resp.DK_DEL, DkDelCommand)
}

func DkExistsCommand(s *resp.Session) error {
	if s.TxCommandQueued {
		return resp.ErrDkTx
	}
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.DK_EXISTS)
	}
	if err := utils.CheckKeySize(len(args[0])); err != nil {
		return err
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.Exists(s, args[0])
		if v, err := redis.Int64(res, err); err != nil {
			return err
		} else {
			s.RespWriter.WriteInteger(v)
		}
	} else {
		return err
	}

	return nil
}

func DkDelCommand(s *resp.Session) error {
	if s.TxCommandQueued {
		return resp.ErrDkTx
	}
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.DK_DEL)
	}
	if !bytes.Equal(args[0], router.DkMainKey) {
		return resp.NotFoundErr
	}
	args = args[1:]

	if proxyClient, err := router.GetProxyClient(); err == nil {
		dkKeys := make([]interface{}, 0, len(args))
		groupKeys := make([]interface{}, 0, len(args))
		for i := 0; i < len(args); i++ {
			if err := utils.CheckKeySize(len(args[i])); err != nil {
				continue
			}
			cacheSn, err := proxyClient.GetDkShardNum(string(args[i]))
			if err != nil {
				return err
			}
			if cacheSn <= 0 {
				continue
			}
			var j uint32
			dkKeys = append(dkKeys, unsafe2.String(args[i]))
			for j = 0; j < cacheSn; j++ {
				k := router.EncodeDkGroupKey(args[i], j)
				groupKeys = append(groupKeys, k)
			}
		}
		if len(groupKeys) <= 0 {
			s.RespWriter.WriteInteger(0)
			return nil
		}
		res, _ := proxyClient.Del(s, dkKeys...)
		n := res.(int64)
		if n <= 0 {
			s.RespWriter.WriteInteger(n)
			return nil
		}
		for _, dk := range dkKeys {
			proxyClient.RemoveSimpleCache(dk)
		}
		res, _ = proxyClient.Del(s, groupKeys...)
		n = res.(int64)
		s.RespWriter.WriteInteger(n)
	} else {
		return err
	}
	return nil
}
