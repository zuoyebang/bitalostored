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
	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/proxy/resp"
	"github.com/zuoyebang/bitalostored/proxy/router"

	"github.com/gomodule/redigo/redis"
)

func init() {
	resp.Register(resp.HSET, HsetCommand)
	resp.Register(resp.HGET, HgetCommand)
	resp.Register(resp.HMSET, HmsetCommand)
	resp.Register(resp.HMGET, HmgetCommand)
	resp.Register(resp.HEXISTS, HexistsCommand)
	resp.Register(resp.HDEL, HdelCommand)
	resp.Register(resp.HINCRBY, HincrbyCommand)
	resp.Register(resp.HLEN, HlenCommand)
	resp.Register(resp.HKEYS, HkeysCommand)
	resp.Register(resp.HVALS, HvalsCommand)
	resp.Register(resp.HGETALL, HgetallCommand)

	resp.Register(resp.HCLEAR, HClearCommand)
	resp.Register(resp.HEXPIRE, HExpireCommand)
	resp.Register(resp.HEXPIREAT, HExpireatCommand)
	resp.Register(resp.HTTL, HTtlCommand)
	resp.Register(resp.HPERSIST, HPersistCommand)
	resp.Register(resp.HKEYEXISTS, HKeyExistsCommand)
}

func HsetCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.HSET)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.HSetWithRes(s, args[0], args[1], args[2])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if n, err := redis.Int64(res, err); err != nil {
				return err
			} else {
				s.RespWriter.WriteInteger(n)
			}
		}
	} else {
		return err
	}

	return nil
}

func HgetCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.HGET)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.HGet(s, args[0], args[1])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if v, err := redis.Bytes(res, err); err != nil && err != redis.ErrNil {
				return err
			} else {
				s.RespWriter.WriteBulk(v)
			}
		}
	} else {
		return err
	}

	return nil
}

func HexistsCommand(s *resp.Session) error {
	args := s.Args

	if len(args) != 2 {
		return resp.CmdParamsErr(resp.HEXISTS)
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.HExists(s, args[0], args[1])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if n, err := redis.Int64(res, err); err != nil {
				return err
			} else {
				s.RespWriter.WriteInteger(n)
			}
		}
	} else {
		return err
	}

	return nil
}

func HdelCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.HDEL)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.HDel(s, args[0], args[1:]...)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if n, err := redis.Int64(res, err); err != nil && err != redis.ErrNil {
				return err
			} else {
				if err == redis.ErrNil {
					n = 0
				}
				s.RespWriter.WriteInteger(n)
			}
		}
	} else {
		return err
	}

	return nil
}

func HlenCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.HLEN)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.HLen(s, args[0])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if n, err := redis.Int64(res, err); err != nil {
				return err
			} else {
				s.RespWriter.WriteInteger(n)
			}
		}
	} else {
		return err
	}

	return nil
}

func HincrbyCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.HINCRBY)
	}

	delta, err := extend.ParseInt64(unsafe2.String(args[2]))
	if err != nil {
		return resp.ValueErr
	}

	var n int64
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.HIncrBy(s, args[0], args[1], delta)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if n, err = redis.Int64(res, err); err != nil {
				return err
			} else {
				s.RespWriter.WriteInteger(n)
			}
		}
	} else {
		return err
	}

	return nil
}

func HmsetCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 3 {
		return resp.CmdParamsErr(resp.HMSET)
	}

	if len(args[1:])%2 != 0 {
		return resp.CmdParamsErr(resp.HMSET)
	}

	key := unsafe2.String(args[0])
	args = args[1:]

	fvMap := make(map[string]interface{}, len(args)/2)
	for i := 0; i < len(args)/2; i++ {
		field := unsafe2.String(args[2*i])
		value := unsafe2.String(args[2*i+1])
		fvMap[field] = value
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		_, err := proxyClient.HMSet(s, key, fvMap)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if err != nil {
				return err
			} else {
				s.RespWriter.WriteStatus(resp.ReplyOK)
			}
		}
	} else {
		return err
	}

	return nil
}

func HmgetCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.HMGET)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.HMGet(s, args[0], args[1:]...)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if v, err := redis.ByteSlices(res, err); err != nil {
				return err
			} else {
				s.RespWriter.WriteSliceArray(v)
			}
		}
	} else {
		return err
	}

	return nil
}

func HkeysCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.HKEYS)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.HKeys(s, args[0])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if v, err := redis.ByteSlices(res, err); err != nil {
				return err
			} else {
				s.RespWriter.WriteSliceArray(v)
			}
		}
	} else {
		return err
	}

	return nil
}

func HvalsCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.HVALS)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.HVals(s, args[0])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if v, err := redis.ByteSlices(res, err); err != nil {
				return err
			} else {
				s.RespWriter.WriteSliceArray(v)
			}
		}
	} else {
		return err
	}

	return nil
}

func HgetallCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.HGETALL)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.HGetAll(s, args[0])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if v, err := redis.ByteSlices(res, err); err != nil {
				return err
			} else {
				s.RespWriter.WriteFVPairArray(v)
			}
		}
	} else {
		return err
	}

	return nil
}

func HClearCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 1 {
		return resp.CmdParamsErr(resp.HCLEAR)
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.HClear(s, args[:]...)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if n, err := redis.Int64(res, err); err != nil && err != redis.ErrNil {
				return err
			} else {
				if err == redis.ErrNil {
					n = 0
				}
				s.RespWriter.WriteInteger(n)
			}
		}
	} else {
		return err
	}

	return nil
}

func HExpireatCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.HEXPIREAT)
	}
	when, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}
	res, err := proxyClient.HExpireAt(s, args[0], when)
	if s.TxCommandQueued {
		return s.SendTxQueued(err)
	} else {
		if v, err := resp.Int64(redis.Bool(res, err)); err != nil {
			return err
		} else {
			s.RespWriter.WriteInteger(v)
		}
	}
	return nil
}

func HPersistCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.HPERSIST)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.HPersist(s, args[0])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if n, err := resp.Int64(redis.Bool(res, err)); err != nil {
				return err
			} else {
				s.RespWriter.WriteInteger(n)
			}
		}
	} else {
		return err
	}

	return nil
}

func HKeyExistsCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.HKEYEXISTS)
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.HKeyExists(s, args[0])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if n, err := resp.Int64(res, err); err != nil {
				return err
			} else {
				s.RespWriter.WriteInteger(n)
			}
		}
	} else {
		return err
	}

	return nil
}

func HTtlCommand(s *resp.Session) error {
	args := s.Args

	if len(args) != 1 {
		return resp.CmdParamsErr(resp.HTTL)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.HTtl(s, args[0])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if v, err := redis.Int64(res, err); err != nil {
				return err
			} else {
				s.RespWriter.WriteInteger(v)
			}
		}
	} else {
		return err
	}

	return nil
}

func HExpireCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.HEXPIRE)
	}

	duration, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.HExpire(s, args[0], duration)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if v, err := resp.Int64(redis.Bool(res, err)); err != nil {
				return err
			} else {
				s.RespWriter.WriteInteger(v)
			}
		}
	} else {
		return err
	}

	return nil
}
