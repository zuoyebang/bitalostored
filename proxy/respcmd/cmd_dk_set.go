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
	"errors"

	"github.com/zuoyebang/bitalostored/proxy/internal/utils"
	"github.com/zuoyebang/bitalostored/proxy/resp"
	"github.com/zuoyebang/bitalostored/proxy/router"

	"github.com/gomodule/redigo/redis"
	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
)

func init() {
	resp.Register(resp.DK_SCREATE, DkScreateCommand)
	resp.Register(resp.DK_SADD, DKSaddCommand)
	resp.Register(resp.DK_SCARD, DkScardCommand)
	resp.Register(resp.DK_SISMEMBER, DkSismemberCommand)
	resp.Register(resp.DK_SREM, DkSremCommand)
	resp.Register(resp.DK_SPOP, DkSpopCommand)
}

func DkScreateCommand(s *resp.Session) error {
	if s.TxCommandQueued {
		return resp.ErrDkTx
	}
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.DK_SCREATE)
	}
	if !bytes.Equal(args[2], router.DkMainKey) {
		return resp.NotFoundErr
	}
	if err := utils.CheckKeySize(len(args[0])); err != nil {
		return err
	}
	shardNum, _ := extend.ParseUint32(unsafe2.String(args[1]))
	if shardNum <= 0 || shardNum > router.MaxDkShardNum {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		cacheSn, err := proxyClient.GetDkShardNum(string(args[0]))
		if err != nil {
			return err
		}
		if cacheSn > 0 {
			return resp.ErrDkKeyDuplicate
		}
		err = proxyClient.CreateDkToServer(args[0], shardNum, "set")
		if err == nil {
			_ = proxyClient.SetDkShardNum(string(args[0]), shardNum)
			s.RespWriter.WriteStatus(resp.ReplyOK)
		} else {
			return err
		}
	} else {
		return err
	}

	return nil
}

func DKSaddCommand(s *resp.Session) error {
	if s.TxCommandQueued {
		return resp.ErrDkTx
	}
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.DK_SADD)
	}
	if err := utils.CheckKeySize(len(args[0])); err != nil {
		return err
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		cacheSn, err := proxyClient.GetDkShardNum(string(args[0]))
		if err != nil {
			return err
		}
		if cacheSn <= 0 {
			return resp.ErrDKkeyNotFound
		}
		res, err := proxyClient.DkSAdd(cacheSn, args...)
		r := res.([]interface{})
		var n int64
		for _, rt := range r {
			switch rt.(type) {
			case int64:
				n += rt.(int64)
			case error:
				return rt.(error)
			}
		}
		if n > 0 {
			_, err := proxyClient.DkIncrBySize(args[0], n)
			if err != nil {
				return err
			}
		}
		s.RespWriter.WriteInteger(n)
	} else {
		return err
	}
	return nil
}

func DkScardCommand(s *resp.Session) error {
	if s.TxCommandQueued {
		return resp.ErrDkTx
	}
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.DK_SCARD)
	}
	if err := utils.CheckKeySize(len(args[0])); err != nil {
		return err
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		_, _, size, _, err := proxyClient.GetDkInfo(unsafe2.String(args[0]))
		if err != nil {
			return err
		}
		s.RespWriter.WriteInteger(size)
	} else {
		return err
	}

	return nil
}

func DkSismemberCommand(s *resp.Session) error {
	if s.TxCommandQueued {
		return resp.ErrDkTx
	}
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.DK_SISMEMBER)
	}
	if err := utils.CheckKeySize(len(args[0])); err != nil {
		return err
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		cacheSn, err := proxyClient.GetDkShardNum(string(args[0]))
		if err != nil {
			return err
		}
		if cacheSn <= 0 {
			s.RespWriter.WriteInteger(0)
			return nil
		}
		hashShard := router.HashDkKey(args[1], cacheSn)
		groupKey := router.EncodeDkGroupKey(args[0], hashShard)
		res, err := proxyClient.DkSIsMember(groupKey, args[1])
		if n, err := resp.Int64(redis.Bool(res, err)); err != nil {
			return err
		} else {
			s.RespWriter.WriteInteger(n)
		}
	} else {
		return err
	}

	return nil
}

func DkSremCommand(s *resp.Session) error {
	if s.TxCommandQueued {
		return resp.ErrDkTx
	}
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.DK_SREM)
	}
	if err := utils.CheckKeySize(len(args[0])); err != nil {
		return err
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		cacheSn, err := proxyClient.GetDkShardNum(string(args[0]))
		if err != nil {
			return err
		}
		if cacheSn <= 0 {
			return resp.ErrDKkeyNotFound
		}
		res, err := proxyClient.DkSRem(cacheSn, args...)
		r := res.([]interface{})
		var n int64
		for _, rt := range r {
			switch rt.(type) {
			case int64:
				n += rt.(int64)
			}
		}
		if n > 0 {
			_, err := proxyClient.DkIncrBySize(args[0], 0-n)
			if err != nil {
				return err
			}
		}
		s.RespWriter.WriteInteger(n)
	} else {
		return err
	}
	return nil
}

func DkSpopCommand(s *resp.Session) error {
	if s.TxCommandQueued {
		return resp.ErrDkTx
	}
	args := s.Args
	if err := utils.CheckKeySize(len(args[0])); err != nil {
		return err
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		cacheSn, err := proxyClient.GetDkShardNum(string(args[0]))
		if err != nil {
			return err
		}
		if cacheSn <= 0 {
			return resp.ErrDKkeyNotFound
		}
		var count int64
		if len(args) == 1 {
			count = 1
		}
		if len(args) == 2 {
			count, err = extend.ParseInt64(unsafe2.String(args[1]))
			if err != nil {
				return resp.ValueErr
			}
		}
		var i uint32
		ret := make([][]byte, 0, count)
		remain := count
		for i = 0; i < cacheSn; i++ {
			if remain <= 0 {
				break
			}
			k := router.EncodeDkGroupKey(args[0], i)
			res, err := proxyClient.DkSPop(unsafe2.ByteSlice(k), remain)
			if res, err := redis.ByteSlices(res, err); err != nil && !errors.Is(err, redis.ErrNil) {
				return err
			} else {
				if errors.Is(err, redis.ErrNil) {
					continue
				}
				ret = append(ret, res...)
				remain = count - int64(len(res))
			}
		}
		proxyClient.DkIncrBySize(args[0], int64(0-len(ret)))
		s.RespWriter.WriteSliceArray(ret)
		return nil
	} else {
		return err
	}
}
