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
	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
)

func init() {
	resp.Register(resp.DK_HCREATE, DkHcreateCommand)
	resp.Register(resp.DK_HGET, DkHgetCommand)
	resp.Register(resp.DK_HSET, DkHsetCommand)
	resp.Register(resp.DK_HMGET, DkHmgetCommand)
	resp.Register(resp.DK_HEXISTS, DkHexistsCommand)
	resp.Register(resp.DK_HDEL, DkHdelCommand)
	resp.Register(resp.DK_HINCRBY, DkHincrbyCommand)
	resp.Register(resp.DK_HLEN, DkHlenCommand)
}

func DkHcreateCommand(s *resp.Session) error {
	if s.TxCommandQueued {
		return resp.ErrDkTx
	}
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.DK_HCREATE)
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
		err = proxyClient.CreateDkToServer(args[0], shardNum, "hash")
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

func DkHgetCommand(s *resp.Session) error {
	if s.TxCommandQueued {
		return resp.ErrDkTx
	}
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.DK_HGET)
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
			s.RespWriter.WriteBulk(nil)
			return nil
		}
		hashShard := router.HashDkKey(args[1], cacheSn)
		groupKey := router.EncodeDkGroupKey(args[0], hashShard)
		res, err := proxyClient.DkHGet(unsafe2.ByteSlice(groupKey), args[1])
		if v, err := redis.Bytes(res, err); err != nil && err != redis.ErrNil {
			return err
		} else {
			s.RespWriter.WriteBulk(v)
		}
	} else {
		return err
	}

	return nil
}

func DkHexistsCommand(s *resp.Session) error {
	if s.TxCommandQueued {
		return resp.ErrDkTx
	}
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.DK_HEXISTS)
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
		res, err := proxyClient.DkHExists(groupKey, args[1])
		if n, err := redis.Int64(res, err); err != nil {
			return err
		} else {
			s.RespWriter.WriteInteger(n)
		}
	} else {
		return err
	}

	return nil
}

func DkHdelCommand(s *resp.Session) error {
	if s.TxCommandQueued {
		return resp.ErrDkTx
	}
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.DK_HDEL)
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
		res, err := proxyClient.DkHDel(cacheSn, args...)
		r := res.([]interface{})
		var n int64
		for _, rt := range r {
			switch rt.(type) {
			case int64:
				n += rt.(int64)
			}
		}
		if n > 0 {
			_, err = proxyClient.DkIncrBySize(args[0], 0-n)
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

func DkHlenCommand(s *resp.Session) error {
	if s.TxCommandQueued {
		return resp.ErrDkTx
	}
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.DK_HLEN)
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

func DkHincrbyCommand(s *resp.Session) error {
	if s.TxCommandQueued {
		return resp.ErrDkTx
	}
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.DK_HINCRBY)
	}
	if err := utils.CheckKeySize(len(args[0])); err != nil {
		return err
	}

	delta, err := extend.ParseInt64(unsafe2.String(args[2]))
	if err != nil {
		return resp.ValueErr
	}

	var n int64
	if proxyClient, err := router.GetProxyClient(); err == nil {
		cacheSn, err := proxyClient.GetDkShardNum(string(args[0]))
		if err != nil {
			return err
		}
		if cacheSn <= 0 {
			return resp.ErrDKkeyNotFound
		}
		hashShard := router.HashDkKey(args[1], cacheSn)
		groupKey := router.EncodeDkGroupKey(args[0], hashShard)
		res, err := proxyClient.DkHIncrBy(groupKey, args[1], delta)
		if n, err = redis.Int64(res, err); err != nil {
			return err
		} else {
			if n == delta {
				proxyClient.DkIncrBySize(args[0], 1)
			}
			s.RespWriter.WriteInteger(n)
		}
	} else {
		return err
	}

	return nil
}

func DkHsetCommand(s *resp.Session) error {
	if s.TxCommandQueued {
		return resp.ErrDkTx
	}
	args := s.Args
	if len(args) < 3 {
		return resp.CmdParamsErr(resp.DK_HSET)
	}
	if err := utils.CheckKeySize(len(args[0])); err != nil {
		return err
	}

	if len(args[1:])%2 != 0 {
		return resp.CmdParamsErr(resp.DK_HSET)
	}

	key := args[0]
	args = args[1:]

	if proxyClient, err := router.GetProxyClient(); err == nil {
		cacheSn, err := proxyClient.GetDkShardNum(string(key))
		if err != nil {
			return err
		}
		if cacheSn <= 0 {
			return resp.ErrDKkeyNotFound
		}
		res, _ := proxyClient.DkHSet(key, cacheSn, args)
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
			proxyClient.DkIncrBySize(key, n)
		}
		s.RespWriter.WriteInteger(n)
	} else {
		return err
	}

	return nil
}

func DkHmgetCommand(s *resp.Session) error {
	if s.TxCommandQueued {
		return resp.ErrDkTx
	}
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.DK_HMGET)
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
			s.RespWriter.WriteSliceArray(nil)
			return nil
		}
		res, err := proxyClient.DkHMGet(cacheSn, args...)
		if v, err := redis.ByteSlices(res, err); err != nil {
			return err
		} else {
			s.RespWriter.WriteSliceArray(v)
		}
	} else {
		return err
	}

	return nil
}
