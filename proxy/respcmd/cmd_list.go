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
	resp.Register(resp.LPUSH, LpushCommand)
	resp.Register(resp.RPUSH, RpushCommand)
	resp.Register(resp.LPOP, LpopCommand)
	resp.Register(resp.RPOP, RpopCommand)
	resp.Register(resp.LLEN, LlenCommand)
	resp.Register(resp.LINDEX, LindexCommand)
	resp.Register(resp.LRANGE, LrangeCommand)
	resp.Register(resp.LREM, LremCommand)
	resp.Register(resp.LINSERT, LinsertCommand)
	resp.Register(resp.LSET, LsetCommand)
	resp.Register(resp.LTRIM, LTrimCommand)
	resp.Register(resp.LPUSHX, LPushXCommand)
	resp.Register(resp.RPUSHX, RPushXCommand)

	resp.Register(resp.LCLEAR, LClearCommand)
	resp.Register(resp.LEXPIRE, LExpireCommand)
	resp.Register(resp.LEXPIREAT, LExpireatCommand)
	resp.Register(resp.LTTL, LTtlCommand)
	resp.Register(resp.LPERSIST, LPersistCommand)
	resp.Register(resp.LKEYEXISTS, LKeyExistsCommand)

	resp.Register(resp.LTRIMBACK, LTrimBackCommand)
	resp.Register(resp.LTRIMFRONT, LTrimFrontCommand)
}

func LTrimBackCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.LTRIMBACK)
	}
	trimSize, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil || trimSize < 0 {
		return resp.ValueErr
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.LTrimBack(s, args[0], trimSize)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if v, err := resp.Int64(redis.Int64(res, err)); err != nil {
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

func LTrimFrontCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.LTRIMFRONT)
	}
	trimSize, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil || trimSize < 0 {
		return resp.ValueErr
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.LTrimFront(s, args[0], trimSize)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if v, err := resp.Int64(redis.Int64(res, err)); err != nil {
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

func LpushCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.LPUSH)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.LPush(s, args[0], args[1:]...)
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

func RpushCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.RPUSH)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.RPush(s, args[0], args[1:]...)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if n, err := redis.Int64(res, err); err != nil {
				return err
			} else {
				s.RespWriter.WriteInteger(int64(n))
			}
		}
	} else {
		return err
	}

	return nil
}

func LpopCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.LPOP)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.LPop(s, args[0])
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

func RpopCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.RPOP)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.RPop(s, args[0])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if v, err := redis.Bytes(res, err); err != nil {
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

func LlenCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.LLEN)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.LLen(s, args[0])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if n, err := resp.Int64(redis.Int64(res, err)); err != nil {
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

func LremCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.LREM)
	}
	count, err := extend.ParseInt(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.LRem(s, args[0], count, unsafe2.String(args[2]))
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if n, err := resp.Int64(redis.Int64(res, err)); err != nil {
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

func LindexCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.LINDEX)
	}

	index, err := extend.ParseInt(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.LIndex(s, args[0], index)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if v, err := redis.Bytes(res, err); err != nil {
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

func LrangeCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.LRANGE)
	}

	var start int
	var stop int
	var err error

	start, err = extend.ParseInt(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	stop, err = extend.ParseInt(unsafe2.String(args[2]))
	if err != nil {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.LRange(s, args[0], start, stop)
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

func LsetCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.LSET)
	}

	index, err := extend.ParseInt(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.LSet(s, args[0], index, args[2])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if _, err := redis.Bytes(res, err); err != nil {
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

func LinsertCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 4 {
		return resp.CmdParamsErr(resp.LINSERT)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.LInsert(s, args[0], unsafe2.String(args[1]), unsafe2.String(args[2]), unsafe2.String(args[3]))
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

func LTrimCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.LTRIM)
	}
	start, err := extend.ParseInt(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	stop, err := extend.ParseInt(unsafe2.String(args[2]))
	if err != nil {
		return resp.ValueErr
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.LTrim(s, args[0], start, stop)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if _, err := redis.Bytes(res, err); err != nil {
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

func LPushXCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.LPUSHX)
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.LPushX(s, args[0], args[1:]...)
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

func RPushXCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.RPUSHX)
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.RPushX(s, args[0], args[1:]...)
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

func LClearCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 1 {
		return resp.CmdParamsErr(resp.LCLEAR)
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.LClear(s, args...)
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

func LExpireatCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.LEXPIREAT)
	}
	when, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}
	res, err := proxyClient.LExpireAt(s, args[0], when)
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

func LPersistCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.LPERSIST)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.LPersist(s, args[0])
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

func LKeyExistsCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.LKEYEXISTS)
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.LKeyExists(s, args[0])
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

func LTtlCommand(s *resp.Session) error {
	args := s.Args

	if len(args) != 1 {
		return resp.CmdParamsErr(resp.LTTL)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.LTtl(s, args[0])
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

func LExpireCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.LEXPIRE)
	}

	duration, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.LExpire(s, args[0], duration)
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
