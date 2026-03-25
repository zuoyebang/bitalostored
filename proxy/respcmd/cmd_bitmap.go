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

	"github.com/zuoyebang/bitalostored/proxy/internal/utils"
	"github.com/zuoyebang/bitalostored/proxy/resp"
	"github.com/zuoyebang/bitalostored/proxy/router"

	"github.com/gomodule/redigo/redis"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
)

func init() {
	resp.Register(resp.GETBIT, GetBitCommand)
	resp.Register(resp.SETBIT, SetBitCommand)
	resp.Register(resp.BITCOUNT, BitCountCommand)
	resp.Register(resp.BITPOS, BitPosCommand)
	resp.Register(resp.GETBIT64, GetBit64Command)
	resp.Register(resp.SETBIT64, SetBit64Command)
	resp.Register(resp.BITCOUNT64, BitCount64Command)
	resp.Register(resp.BITPOS64, BitPos64Command)
}

func BitCountCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 && len(args) != 3 {
		return resp.CmdParamsErr(resp.BITCOUNT)
	}
	if err := utils.CheckKeySize(len(args[0])); err != nil {
		return err
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
	if err := utils.CheckKeySize(len(args[0])); err != nil {
		return err
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

func GetBit64Command(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.GETBIT64)
	}
	if err := utils.CheckKeySize(len(args[0])); err != nil {
		return err
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
		res, err := proxyClient.GetBit64(s, key, offset)
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

func SetBit64Command(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.SETBIT64)
	}
	if err := utils.CheckKeySize(len(args[0])); err != nil {
		return err
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
		res, err := proxyClient.SetBit64(s, key, offset, value)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if v, err := redis.Int64(res, err); err != nil {
				return err
			} else {
				s.RespWriter.WriteInteger(v)
				return nil
			}
		}
	} else {
		return err
	}
}

func BitCount64Command(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 && len(args) != 3 {
		return resp.CmdParamsErr(resp.BITCOUNT64)
	}
	if err := utils.CheckKeySize(len(args[0])); err != nil {
		return err
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
			res, err = proxyClient.BitCount64(s, key)
			if s.TxCommandQueued {
				return s.SendTxQueued(err)
			} else {
				v, err = redis.Int64(res, err)
			}
		} else {
			res, err = proxyClient.BitCount64(s, key, start, end)
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

func BitPos64Command(s *resp.Session) error {
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.BITPOS64)
	}
	if err := utils.CheckKeySize(len(args[0])); err != nil {
		return err
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
		res, err := proxyClient.BitPos64(s, key, bit, start, end)
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
