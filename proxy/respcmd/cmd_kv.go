// Copyright 2019 The Bitalostored author and other contributors.
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
	"strconv"
	"time"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/proxy/resp"
	"github.com/zuoyebang/bitalostored/proxy/router"

	"github.com/gomodule/redigo/redis"
)

func init() {
	resp.Register(resp.SET, SetCommand)
	resp.Register(resp.SETEX, SetExCommand)
	resp.Register(resp.PSETEX, PSetExCommand)
	resp.Register(resp.SETNX, SetNxCommand)
	resp.Register(resp.GET, GetCommand)
	resp.Register(resp.GETSET, GetSetCommand)
	resp.Register(resp.MSET, MSetCommand)
	resp.Register(resp.MGET, MgetCommand)
	resp.Register(resp.INCR, IncrCommand)
	resp.Register(resp.INCRBY, IncrbyCommand)
	resp.Register(resp.INCRBYFLOAT, IncrByFloatCommand)
	resp.Register(resp.DECR, DecrCommand)
	resp.Register(resp.DECRBY, DecrbyCommand)
	resp.Register(resp.EXISTS, ExistsCommand)
	resp.Register(resp.DEL, DelCommand)
	resp.Register(resp.UNLINK, DelCommand)
	resp.Register(resp.EXPIRE, ExpireCommand)
	resp.Register(resp.PERSIST, PersistCommand)
	resp.Register(resp.TTL, TtlCommand)
	resp.Register(resp.PTTL, PTtlCommand)
	resp.Register(resp.TYPE, TypeCommand)

	resp.Register(resp.SELECT, SelectCommand)

	resp.Register(resp.EXPIREAT, ExpireAtCommand)
	resp.Register(resp.PEXPIRE, PExpireCommand)
	resp.Register(resp.PEXPIREAT, PExpireAtCommand)
	resp.Register(resp.KDEL, KDelCommand)
	resp.Register(resp.KEXISTS, KExistsCommand)
	resp.Register(resp.KEXPIRE, KExpireCommand)
	resp.Register(resp.KEXPIREAT, KExpireAtCommand)
	resp.Register(resp.KPERSIST, KPersistCommand)
	resp.Register(resp.KTTL, KTtlCommand)

	resp.Register(resp.APPEND, AppendCommand)
	resp.Register(resp.GETRANGE, GetRangeCommand)
	resp.Register(resp.SETRANGE, SetRangeCommand)
	resp.Register(resp.STRLEN, StrLenCommand)

	resp.Register(resp.GETBIT, GetBitCommand)
	resp.Register(resp.SETBIT, SetBitCommand)
	resp.Register(resp.BITCOUNT, BitCountCommand)
	resp.Register(resp.BITPOS, BitPosCommand)

	resp.Register(resp.PKSETEXAT, PKSETEXATCommand)
}

func AppendCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.APPEND)
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.Append(s, unsafe2.String(args[0]), unsafe2.String(args[1]))
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if index, err := resp.Int64(res, err); err != nil {
				return err
			} else {
				s.RespWriter.WriteInteger(index)
			}
		}
	} else {
		return err
	}

	return nil
}

func KPersistCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.KPERSIST)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.KPersist(s, args[0])
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

func KExpireAtCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.KEXPIREAT)
	}
	when, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}
	res, err := proxyClient.KExpireAt(s, args[0], when)
	if s.TxCommandQueued {
		return s.SendTxQueued(err)
	} else {
		if v, err := redis.Int64(res, err); err != nil {
			return err
		} else {
			s.RespWriter.WriteInteger(v)
		}
	}
	return nil
}

func KExpireCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.KEXPIRE)
	}

	duration, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.KExpire(s, unsafe2.String(args[0]), duration)
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

func KTtlCommand(s *resp.Session) error {
	args := s.Args

	if len(args) != 1 {
		return resp.CmdParamsErr(resp.KTTL)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.KTtl(s, args[0])
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

func KExistsCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.KEXISTS)
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.KExists(s, args[0])
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

func KDelCommand(s *resp.Session) error {
	args := s.Args
	if len(args) == 0 {
		return resp.CmdParamsErr(resp.KDEL)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.KDel(s, resp.StringSlice(args[:])...)
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

func GetCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.GET)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.Get(s, unsafe2.String(args[0]))
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if err != nil {
				return err
			} else {
				if res == nil {
					s.RespWriter.WriteBulk(nil)
				} else {
					if r, ok := res.([]byte); ok {
						s.RespWriter.WriteBulk(r)
					} else {
						s.RespWriter.WriteBulk(nil)
					}
				}
			}
		}
	} else {
		return err
	}

	return nil
}

func SetCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.SET)
	}

	exType, t, setCondition, err := resp.ParseSetArgs(args[2:])
	if err != nil {
		return err
	}
	var proxyClient *router.ProxyClient
	if proxyClient, err = router.GetProxyClient(); err != nil {
		return err
	}
	resetNx := func(r interface{}, err error) (bool, error) {
		_, err = redis.String(r, err)
		if err == nil {
			return true, nil
		} else {
			if err == redis.ErrNil {
				err = nil
			}
			return false, err
		}
	}
	var res bool
	if exType == resp.NoType && setCondition == resp.NoCondition {
		_, err = proxyClient.Set(s, unsafe2.String(args[0]), unsafe2.String(args[1]), exType, 0)
		res = true
	} else if exType == resp.NoType && setCondition == resp.NX {
		var res1 interface{}
		res1, err = proxyClient.SetNx(s, args[0], args[1])
		if !s.TxCommandQueued {
			res, err = redis.Bool(res1, err)
			if err == redis.ErrNil {
				res = false
				err = nil
			}
		}
	} else if exType == resp.EX && setCondition == resp.NoCondition {
		_, err = proxyClient.Set(s, unsafe2.String(args[0]), unsafe2.String(args[1]), exType, t)
		res = true
	} else if exType == resp.PX && setCondition == resp.NoCondition {
		_, err = proxyClient.Set(s, unsafe2.String(args[0]), unsafe2.String(args[1]), exType, t)
		res = true
	} else if exType == resp.EX && setCondition == resp.NX {
		var res1 interface{}
		res1, err = proxyClient.SetNxByEX(s, args[0], args[1], uint64(t))
		if !s.TxCommandQueued {
			res, err = resetNx(res1, err)
		}
	} else if exType == resp.PX && setCondition == resp.NX {
		var res1 interface{}
		res1, err = proxyClient.SetNxByPX(s, args[0], args[1], uint64(t))
		if !s.TxCommandQueued {
			res, err = resetNx(res1, err)
		}
	} else {
		err = resp.NotImplementErr
	}

	if err != nil {
		return err
	} else {
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if res {
				s.RespWriter.WriteStatus(resp.ReplyOK)
			} else {
				s.RespWriter.WriteBulk(nil)
			}
		}
	}
	return nil
}

func GetSetCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.GETSET)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.GetSet(s, unsafe2.String(args[0]), unsafe2.String(args[1]))
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

func PKSETEXATCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.SETEX)
	}
	expireUnixTime, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	expireTime := expireUnixTime - time.Now().Unix()
	if expireTime <= 0 {
		return nil
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		_, err := proxyClient.Set(s, unsafe2.String(args[0]), unsafe2.String(args[2]), resp.EX, expireTime)
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

func SetExCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.SETEX)
	}
	sec, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		_, err := proxyClient.Set(s, unsafe2.String(args[0]), unsafe2.String(args[2]), resp.EX, sec)
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

func PSetExCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.PSETEX)
	}
	msec, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		_, err := proxyClient.Set(s, unsafe2.String(args[0]), unsafe2.String(args[2]), resp.PX, msec)
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

func SetNxCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.SETNX)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.SetNx(s, args[0], args[1])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if n, err := resp.Int64(redis.Bool(res, err)); err != nil && err != redis.ErrNil {
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

func ExistsCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.EXISTS)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.Exists(s, args[0])
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

func IncrCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.INCR)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.Incr(s, s.Args[0])
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

func DecrCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.DECR)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.Decr(s, s.Args[0])
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

func IncrbyCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.INCRBY)
	}

	delta, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.IncrBy(s, s.Args[0], delta)
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

func DecrbyCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.DECRBY)
	}

	delta, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.DecrBy(s, s.Args[0], delta)
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

func IncrByFloatCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.INCRBYFLOAT)
	}

	score, err := extend.ParseFloat64(unsafe2.String(args[1]))
	if err != nil {
		return resp.FloatErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.IncrByFloat(s, s.Args[0], score)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if b, err := redis.Bytes(res, err); err != nil {
				return err
			} else {
				s.RespWriter.WriteBulk(b)
			}
		}
	} else {
		return err
	}

	return nil
}

func DelCommand(s *resp.Session) error {
	args := s.Args
	if len(args) == 0 {
		return resp.CmdParamsErr(resp.DEL)
	}
	keys := make([]interface{}, 0, len(args))
	for _, value := range resp.StringSlice(args) {
		keys = append(keys, value)
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.Del(s, keys...)
		if err != nil {
			return err
		}
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

func SelectCommand(s *resp.Session) error {
	s.RespWriter.WriteStatus("OK")
	return nil
}

func MSetCommand(s *resp.Session) error {
	args := s.Args
	if len(args) == 0 || len(args)%2 != 0 {
		return resp.CmdParamsErr(resp.MSET)
	}

	kvs := resp.StringSlice(args)
	if proxyClient, err := router.GetProxyClient(); err == nil {
		_, err := proxyClient.MSet(s, kvs...)
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

func MgetCommand(s *resp.Session) error {
	args := s.Args
	if len(args) == 0 {
		return resp.CmdParamsErr("mget")
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, _ := proxyClient.MGet(s, args)
		if s.TxCommandQueued {
			return s.SendTxQueued(nil)
		} else {
			v, _ := redis.ByteSlices(res, nil)
			s.RespWriter.WriteSliceArray(v)
		}
	} else {
		return err
	}
	return nil
}

func ExpireCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.EXPIRE)
	}

	duration, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.Expire(s, unsafe2.String(args[0]), duration)
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

func PExpireCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.PEXPIRE)
	}

	duration, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.PExpire(s, unsafe2.String(args[0]), duration)
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

func PExpireAtCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.PEXPIREAT)
	}
	when, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}
	res, err := proxyClient.ExpireAt(s, resp.PEXPIREAT, args[0], when)
	if s.TxCommandQueued {
		return s.SendTxQueued(err)
	} else {
		if v, err := redis.Int64(res, err); err != nil {
			return err
		} else {
			s.RespWriter.WriteInteger(v)
		}
	}
	return nil
}

func ExpireAtCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.EXPIREAT)
	}
	when, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}
	res, err := proxyClient.ExpireAt(s, resp.EXPIRE, args[0], when)
	if s.TxCommandQueued {
		return s.SendTxQueued(err)
	} else {
		if v, err := redis.Int64(res, err); err != nil {
			return err
		} else {
			s.RespWriter.WriteInteger(v)
		}
	}
	return nil
}

func TtlCommand(s *resp.Session) error {
	args := s.Args

	if len(args) != 1 {
		return resp.CmdParamsErr(resp.TTL)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.TTL(resp.TTL, s, args[0])
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

func PTtlCommand(s *resp.Session) error {
	args := s.Args

	if len(args) != 1 {
		return resp.CmdParamsErr(resp.PTTL)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.TTL(resp.PTTL, s, args[0])
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

func PersistCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.PERSIST)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.Persist(s, args[0])
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

func TypeCommand(s *resp.Session) error {
	args := s.Args

	if len(args) != 1 {
		return resp.CmdParamsErr(resp.TYPE)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.Type(args[0], s)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if v, err := redis.Bytes(res, err); err != nil {
				return err
			} else {
				s.RespWriter.WriteStatus(unsafe2.String(v))
			}
		}
	} else {
		return err
	}

	return nil
}

func GetRangeCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.GETRANGE)
	}
	key := args[0]
	start, end, err := parseBitRange(args[1:])
	if err != nil {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.GetRange(s, key, start, end)
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

func SetRangeCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.SETRANGE)
	}

	key := unsafe2.String(args[0])
	offset, err := strconv.Atoi(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	if offset < 0 {
		return resp.RangeOffsetErr
	}

	value := unsafe2.String(args[2])

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.SetRange(s, key, offset, value)
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

func StrLenCommand(s *resp.Session) error {
	if len(s.Args) != 1 {
		return resp.CmdParamsErr(resp.STRLEN)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.StrLen(s, s.Args[0])
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

func GetBitCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.GETBIT)
	}

	key := args[0]
	offset, err := strconv.Atoi(unsafe2.String(args[1]))
	if err != nil {
		return err
	}
	if offset < 0 {
		return resp.BitOffsetErr
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.GetBit(s, key, offset)
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

func SetBitCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.SETBIT)
	}

	key := args[0]
	offset, err := strconv.Atoi(unsafe2.String(args[1]))
	if err != nil {
		return err
	}
	if offset < 0 {
		return resp.BitOffsetErr
	}

	value, err := strconv.Atoi(unsafe2.String(args[2]))
	if err != nil {
		return err
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.SetBit(s, key, offset, value)
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

func BitCountCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 && len(args) != 3 {
		return resp.CmdParamsErr(resp.SETBIT)
	}

	var start, end int
	var err error
	key := args[0]

	if len(args) == 3 {
		start, end, err = parseBitRange(args[1:])
		if err != nil {
			return resp.ValueErr
		}
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		var v int64
		var err error
		var res interface{}

		if len(args) == 1 {
			res, err = proxyClient.BitCount(s, key)
			if s.TxCommandQueued {
				return s.SendTxQueued(err)
			} else {
				v, err = redis.Int64(res, err)
			}
		} else {
			res, err = proxyClient.BitCount(s, key, start, end)
			if s.TxCommandQueued {
				return s.SendTxQueued(err)
			} else {
				v, err = redis.Int64(res, err)
			}

		}
		if err != nil {
			return err
		} else {
			s.RespWriter.WriteInteger(v)
		}
	} else {
		return err
	}
	return nil
}

func BitPosCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.BITPOS)
	}

	key := args[0]
	bit, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	start, end, err := parseBitRange(args[2:])
	if err != nil {
		return resp.ValueErr
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.BitPos(s, key, bit, start, end)
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

func parseBitRange(args [][]byte) (start int, end int, err error) {
	start = 0
	end = -1
	if len(args) > 0 {
		if start, err = strconv.Atoi(unsafe2.String(args[0])); err != nil {
			return
		}
	}

	if len(args) == 2 {
		if end, err = strconv.Atoi(unsafe2.String(args[1])); err != nil {
			return
		}
	}
	return
}
