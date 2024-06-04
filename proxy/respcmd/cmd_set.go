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
	"strconv"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/proxy/resp"
	"github.com/zuoyebang/bitalostored/proxy/router"

	"github.com/gomodule/redigo/redis"
)

func init() {
	resp.Register(resp.SADD, SaddCommand)
	resp.Register(resp.SCARD, ScardCommand)
	resp.Register(resp.SISMEMBER, SismemberCommand)
	resp.Register(resp.SMEMBERS, SmembersCommand)
	resp.Register(resp.SREM, SremCommand)
	resp.Register(resp.SPOP, SpopCommand)
	resp.Register(resp.SRANDMEMBER, SRandMemberCommand)

	resp.Register(resp.SCLEAR, SClearCommand)
	resp.Register(resp.SEXPIRE, SExpireCommand)
	resp.Register(resp.SEXPIREAT, SExpireatCommand)
	resp.Register(resp.STTL, STtlCommand)
	resp.Register(resp.SPERSIST, SPersistCommand)
	resp.Register(resp.SKEYEXISTS, SKeyExistsCommand)
}

func SRandMemberCommand(s *resp.Session) error {
	args := s.Args
	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}
	if len(args) == 2 {
		count, err := strconv.Atoi(unsafe2.String(args[1]))
		if err != nil {
			return resp.ValueErr
		}
		res, err := proxyClient.SRandMemberCommandWithCount(s, args[0], count)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if v, err := redis.ByteSlices(res, err); err != nil {
				return err
			} else {
				s.RespWriter.WriteSliceArray(v)
				return nil
			}
		}
	}
	if len(args) == 1 {
		res, err := proxyClient.SRandMemberCommandWithoutCount(s, args[0])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if v, err := redis.Bytes(res, err); err != nil {
				return err
			} else {
				s.RespWriter.WriteBulk(v)
				return nil
			}
		}
	}
	return resp.CmdParamsErr(resp.SRANDMEMBER)
}

func SaddCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.SADD)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.SAdd(s, args[0], args[1:]...)
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

func ScardCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.SCARD)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.SCard(s, args[0])
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

func SismemberCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.SISMEMBER)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.SIsMember(s, args[0], args[1])
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

func SmembersCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.SMEMBERS)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.SMembers(s, args[0])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if v, err := redis.ByteSlices(res, err); err != nil && err != redis.ErrNil {
				return err
			} else {
				if err == redis.ErrNil {
					v = nil
				}
				s.RespWriter.WriteSliceArray(v)
			}
		}
	} else {
		return err
	}

	return nil
}

func SremCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.SREM)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.SRem(s, args[0], args[1:]...)
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

func SClearCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 1 {
		return resp.CmdParamsErr(resp.SCLEAR)
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.SClear(s, args[:]...)
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

func SExpireatCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.SEXPIREAT)
	}
	when, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}
	res, err := proxyClient.SExpireAt(s, args[0], when)
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

func SPersistCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.SPERSIST)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.SPersist(s, args[0])
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

func SKeyExistsCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.SKEYEXISTS)
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.SKeyExists(s, args[0])
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

func STtlCommand(s *resp.Session) error {
	args := s.Args

	if len(args) != 1 {
		return resp.CmdParamsErr(resp.STTL)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.STtl(s, args[0])
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

func SExpireCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.SEXPIRE)
	}

	duration, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.SExpire(s, args[0], duration)
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

func SpopCommand(s *resp.Session) error {
	args := s.Args

	if proxyClient, err := router.GetProxyClient(); err == nil {
		if len(args) == 1 {
			res, err := proxyClient.SPop(s, args[0])
			if s.TxCommandQueued {
				return s.SendTxQueued(err)
			} else {
				if res, err := redis.Bytes(res, err); err != nil && err != redis.ErrNil {
					return err
				} else {
					if err == redis.ErrNil {
						res = nil
					}
					s.RespWriter.WriteBulk(res)
					return nil
				}
			}
		}
		if len(args) == 2 {
			count, err := extend.ParseInt64(unsafe2.String(args[1]))
			if err != nil {
				return resp.ValueErr
			}
			res, err := proxyClient.SPopByCount(s, args[0], count)
			if s.TxCommandQueued {
				return s.SendTxQueued(err)
			} else {
				if res, err := redis.ByteSlices(res, err); err != nil && err != redis.ErrNil {
					return err
				} else {
					if err == redis.ErrNil {
						res = nil
					}
					s.RespWriter.WriteSliceArray(res)
					return nil
				}
			}
		}

		return resp.CmdParamsErr(resp.SPOP)
	} else {
		return err
	}
}
