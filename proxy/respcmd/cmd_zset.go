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
	"errors"
	"strconv"
	"strings"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/proxy/resp"
	"github.com/zuoyebang/bitalostored/proxy/router"

	"github.com/gomodule/redigo/redis"
)

func init() {
	resp.Register(resp.ZADD, ZaddCommand)
	resp.Register(resp.ZSCORE, ZscoreCommand)
	resp.Register(resp.ZCARD, ZcardCommand)
	resp.Register(resp.ZCOUNT, ZcountCommand)
	resp.Register(resp.ZINCRBY, ZincrbyCommand)
	resp.Register(resp.ZRANGE, ZrangeCommand)
	resp.Register(resp.ZRANGEBYSCORE, ZrangebyscoreCommand)
	resp.Register(resp.ZRANK, ZrankCommand)
	resp.Register(resp.ZREM, ZremCommand)
	resp.Register(resp.ZREMRANGEBYRANK, ZremrangebyrankCommand)
	resp.Register(resp.ZREMRANGEBYSCORE, ZremrangebyscoreCommand)
	resp.Register(resp.ZREVRANGE, ZrevrangeCommand)
	resp.Register(resp.ZREVRANK, ZrevrankCommand)
	resp.Register(resp.ZREVRANGEBYSCORE, ZrevrangebyscoreCommand)
	resp.Register(resp.ZREMRANGEBYLEX, ZremrangebylexCommand)
	resp.Register(resp.ZLEXCOUNT, ZlexcountCommand)
	resp.Register(resp.ZCLEAR, ZClearCommand)
	resp.Register(resp.ZEXPIRE, ZExpireCommand)
	resp.Register(resp.ZEXPIREAT, ZExpireatCommand)
	resp.Register(resp.ZTTL, ZTtlCommand)
	resp.Register(resp.ZPERSIST, ZPersistCommand)
	resp.Register(resp.ZKEYEXISTS, ZKeyExistsCommand)
	resp.Register(resp.ZRANGEBYLEX, ZRangeByLexCommand)
}

func ZaddCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 3 {
		return resp.CmdParamsErr(resp.ZADD)
	}

	key := unsafe2.String(args[0])
	if len(args[1:])&1 != 0 {
		return resp.CmdParamsErr(resp.ZADD)
	}

	args = args[1:]

	paramsMap := make(map[string]float64, len(args)>>1)
	for i := 0; i < len(args)>>1; i++ {
		score, err := extend.ParseFloat64(unsafe2.String(args[2*i]))
		if err != nil {
			return resp.FloatErr
		}
		member := unsafe2.String(args[2*i+1])
		paramsMap[member] = score
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.ZAdd(s, key, paramsMap)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			n, err := redis.Int64(res, err)
			if err == nil {
				s.RespWriter.WriteInteger(n)
			} else {
				return err
			}
		}
	} else {
		return err
	}

	return nil
}

func ZcardCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.ZCARD)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.ZCard(s, args[0])
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

func ZscoreCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.ZSCORE)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.ZScore(s, args[0], args[1])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if score, err := redis.String(res, err); err != nil && err != redis.ErrNil {
				return err
			} else {
				if err == redis.ErrNil {
					score = ""
				}
				if len(score) <= 0 {
					s.RespWriter.WriteBulk(nil)
				} else {
					s.RespWriter.WriteBulk(unsafe2.ByteSlice(score))
				}
			}
		}
	} else {
		return err
	}

	return nil
}

func ZremCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.ZREM)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.ZRem(s, args[0], args[1:]...)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			n, err := redis.Int64(res, err)
			if err == redis.ErrNil {
				n = 0
				err = nil
			}
			if err == nil {
				s.RespWriter.WriteInteger(n)
			} else {
				return err
			}
		}
	} else {
		return err
	}

	return nil
}

func ZincrbyCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.ZINCRBY)
	}

	key := args[0]
	delta, err := extend.ParseFloat64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.ZIncrBy(s, key, delta, args[2])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			v, err := redis.Float64(res, err)
			if err == nil {
				s.RespWriter.WriteBulk(extend.FormatFloat64ToSlice(v))
			} else {
				return err
			}
		}
	} else {
		return err
	}

	return nil
}

func zparseScoreRange(minBuf []byte, maxBuf []byte) (isscoreWRong bool, err error) {
	var min, max float64
	var minInf, maxInf bool
	if strings.ToLower(unsafe2.String(minBuf)) != "-inf" {
		if len(minBuf) == 0 {
			err = resp.SyntaxErr
			return
		}
		tmpminBuf := minBuf
		if minBuf[0] == '(' {
			tmpminBuf = minBuf[1:]
		}

		if min, err = extend.ParseFloat64(unsafe2.String(tmpminBuf)); err != nil {
			err = resp.ValueErr
			return
		}
	} else {
		minInf = true
	}

	if strings.ToLower(unsafe2.String(maxBuf)) != "+inf" {
		if len(maxBuf) == 0 {
			err = resp.SyntaxErr
			return
		}
		tmpmaxBuf := maxBuf
		if maxBuf[0] == '(' {
			tmpmaxBuf = maxBuf[1:]
		}
		if max, err = extend.ParseFloat64(unsafe2.String(tmpmaxBuf)); err != nil {
			err = resp.ValueErr
			return
		}
	} else {
		maxInf = true
	}

	if !minInf && !maxInf && min > max {
		return true, errors.New("score val error")
	}

	return
}

func ZcountCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.ZCOUNT)
	}

	isScoreWrong, err := zparseScoreRange(args[1], args[2])
	if isScoreWrong {
		s.RespWriter.WriteInteger(0)
		return nil
	}

	if err != nil {
		return resp.ValueErr
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.ZCount(s, unsafe2.String(args[0]), unsafe2.String(args[1]), unsafe2.String(args[2]))
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if n, err := redis.Int64(res, err); err != nil && err != redis.ErrNil {
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

func ZrankCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.ZRANK)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.ZRank(s, args[0], args[1])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if n, err := redis.Int64(res, err); err != nil {
				return err
			} else if n == -1 {
				s.RespWriter.WriteBulk(nil)
			} else {
				s.RespWriter.WriteInteger(n)
			}
		}
	} else {
		return err
	}

	return nil
}

func ZrevrankCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.ZREVRANK)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.ZRevRank(s, args[0], args[1])
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if n, err := redis.Int64(res, err); err != nil {
				return err
			} else if n == -1 {
				s.RespWriter.WriteBulk(nil)
			} else {
				s.RespWriter.WriteInteger(n)
			}
		}
	} else {
		return err
	}

	return nil
}

func ZremrangebyrankCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.ZREMRANGEBYRANK)
	}

	key := unsafe2.String(args[0])

	start, stop, err := zparseRange(s, args[1], args[2])
	if err != nil {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.ZRemRangeByRank(s, key, start, stop)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			n, err := redis.Int64(res, err)
			if err == nil || err == redis.ErrNil {
				s.RespWriter.WriteInteger(n)
			} else {
				return err
			}
		}
	} else {
		return err
	}

	return nil
}

func ZremrangebyscoreCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.ZREMRANGEBYSCORE)
	}

	key := unsafe2.String(args[0])
	isScoreWrong, err := zparseScoreRange(args[1], args[2])
	if isScoreWrong {
		s.RespWriter.WriteInteger(0)
		return nil
	}
	if err != nil {
		return err
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.ZRemRangeByScore(s, key, unsafe2.String(args[1]), unsafe2.String(args[2]))
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			n, err := redis.Int64(res, err)
			if err == nil || err == redis.ErrNil {
				s.RespWriter.WriteInteger(n)
			} else {
				return err
			}
		}
	} else {
		return err
	}

	return nil
}

func zparseRange(s *resp.Session, a1 []byte, a2 []byte) (start int, stop int, err error) {
	if start, err = strconv.Atoi(unsafe2.String(a1)); err != nil {
		return
	}

	if stop, err = strconv.Atoi(unsafe2.String(a2)); err != nil {
		return
	}

	return
}

func zrangeGeneric(s *resp.Session, reverse bool) error {
	args := s.Args
	if len(args) < 3 {
		if reverse {
			return resp.CmdParamsErr(resp.ZREVRANGE)
		} else {
			return resp.CmdParamsErr(resp.ZRANGE)
		}

	}

	key := unsafe2.String(args[0])

	start, stop, err := zparseRange(s, args[1], args[2])
	if err != nil {
		return resp.ValueErr
	}

	args = args[3:]
	var withScores bool = false

	if len(args) > 0 {
		if len(args) != 1 {
			return resp.CmdParamsErr(resp.ZRANGE)
		}
		if strings.ToLower(unsafe2.String(args[0])) == "withscores" {
			withScores = true
		} else {
			return resp.SyntaxErr
		}
	}

	var proxyClient *router.ProxyClient
	if proxyClient, err = router.GetProxyClient(); err != nil {
		return err
	}

	var res interface{}
	var datas [][]byte
	if reverse {
		res, err = proxyClient.ZRevRange(s, key, start, stop, withScores)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			datas, err = redis.ByteSlices(res, err)
			if err == redis.ErrNil {
				err = nil
			}
		}
	} else {
		res, err = proxyClient.ZRange(s, key, start, stop, withScores)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			datas, err = redis.ByteSlices(res, err)
			if err == redis.ErrNil {
				err = nil
			}
		}
	}
	if err != nil {
		return err
	} else {
		s.RespWriter.WriteScorePairArray(datas, withScores)
	}
	return nil
}

func ZrangeCommand(s *resp.Session) error {
	return zrangeGeneric(s, false)
}

func ZrevrangeCommand(s *resp.Session) error {
	return zrangeGeneric(s, true)
}

func ZrangebyscoreGeneric(s *resp.Session, reverse bool) error {
	args := s.Args
	if len(args) < 3 {
		if reverse {
			return resp.CmdParamsErr(resp.ZREVRANGEBYSCORE)
		} else {
			return resp.CmdParamsErr(resp.ZRANGEBYSCORE)
		}
	}
	key := unsafe2.String(args[0])

	var minScore, maxScore []byte

	if reverse {
		minScore, maxScore = args[2], args[1]
	} else {
		minScore, maxScore = args[1], args[2]
	}

	isScoreWrong, err := zparseScoreRange(minScore, maxScore)
	if isScoreWrong {
		s.RespWriter.WriteInteger(0)
		return nil
	}

	if err != nil {
		return err
	}
	args = args[3:]

	var withScores bool = false

	if len(args) > 0 {
		if strings.ToLower(unsafe2.String(args[0])) == "withscores" {
			withScores = true
			args = args[1:]
		}
	}

	var limit bool = false
	var offset int = 0
	var count int = -1
	if len(args) > 0 {
		if len(args) < 3 {
			if reverse {
				return resp.CmdParamsErr(resp.ZREVRANGEBYSCORE)
			} else {
				return resp.CmdParamsErr(resp.ZRANGEBYSCORE)
			}
		}

		if strings.ToLower(unsafe2.String(args[0])) != "limit" {
			return resp.SyntaxErr
		}

		if offset, err = strconv.Atoi(unsafe2.String(args[1])); err != nil {
			return resp.ValueErr
		}

		if count, err = strconv.Atoi(unsafe2.String(args[2])); err != nil {
			return resp.ValueErr
		}

		if len(args) == 4 {
			if strings.ToLower(unsafe2.String(args[3])) == "withscores" {
				withScores = true
			}
		}
		limit = true
	}

	if offset < 0 {
		s.RespWriter.WriteArray([]interface{}{})
		return nil
	}
	var proxyClient *router.ProxyClient
	if proxyClient, err = router.GetProxyClient(); err != nil {
		return err
	}

	var res interface{}
	var datas [][]byte
	if reverse {
		res, err = proxyClient.ZRevRangeByScore(s, key, unsafe2.String(maxScore), unsafe2.String(minScore), withScores, limit, offset, count)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			datas, err = redis.ByteSlices(res, err)
			if err == redis.ErrNil {
				err = nil
			}
		}
	} else {
		res, err = proxyClient.ZRangeByScore(s, key, unsafe2.String(minScore), unsafe2.String(maxScore), withScores, limit, offset, count)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			datas, err = redis.ByteSlices(res, err)
			if err == redis.ErrNil {
				err = nil
			}
		}
	}
	if err != nil {
		return err
	} else {
		s.RespWriter.WriteScorePairArray(datas, withScores)
	}

	return nil
}

func ZrangebyscoreCommand(s *resp.Session) error {
	return ZrangebyscoreGeneric(s, false)
}

func ZrevrangebyscoreCommand(s *resp.Session) error {
	return ZrangebyscoreGeneric(s, true)
}

/*
func zunionstoreCommand(c *session) error {
	args := s.Args
	if len(args) < 2 {
		return CmdParamsErr
	}

	destKey, srcKeys, weights, aggregate, err := zparseZsetoptStore(args)
	if err != nil {
		return err
	}

	n, err := proxyClient.ZUnionStore(destKey, srcKeys, weights, aggregate)

	if err == nil {
		s.RespWriter.WriteInteger(n)
	}

	return err
}*/
/*
func zinterstoreCommand(c *session) error {
	args := s.Args
	if len(args) < 2 {
		return CmdParamsErr
	}

	destKey, srcKeys, weights, aggregate, err := zparseZsetoptStore(args)
	if err != nil {
		return err
	}

	n, err := proxyClient.ZInterStore(destKey, srcKeys, weights, aggregate)

	if err == nil {
		s.RespWriter.WriteInteger(n)
	}

	return err
}*/

func zparseMemberRange(minBuf []byte, maxBuf []byte) (min []byte, max []byte, rangeType uint8, err error) {
	rangeType = resp.RangeClose
	if strings.ToLower(unsafe2.String(minBuf)) == "-" {
		min = minBuf
	} else {
		if len(minBuf) == 0 {
			err = resp.CmdParamsErr(resp.ZLEXCOUNT)
			return
		}

		if minBuf[0] == '(' {
			rangeType |= resp.RangeLopen
			min = minBuf
		} else if minBuf[0] == '[' {
			min = minBuf
		} else {
			err = resp.SyntaxErr
			return
		}
	}

	if strings.ToLower(unsafe2.String(maxBuf)) == "+" {
		max = maxBuf
	} else {
		if len(maxBuf) == 0 {
			err = resp.SyntaxErr
			return
		}
		if maxBuf[0] == '(' {
			rangeType |= resp.RangeRopen
			max = maxBuf
		} else if maxBuf[0] == '[' {
			max = maxBuf
		} else {
			err = resp.SyntaxErr
			return
		}
	}

	return
}

/*
	func zrangebylexCommand(c *session) error {
		args := s.Args
		if len(args) != 3 && len(args) != 6 {
			return CmdParamsErr
		}

		min, max, rangeType, err := zparseMemberRange(args[1], args[2])
		if err != nil {
			return err
		}

		var offset int = 0
		var count int = -1

		if len(args) == 6 {
			if strings.ToLower(unsafe2.String(args[3])) != "limit" {
				return SyntaxErr
			}

			if offset, err = strconv.Atoi(unsafe2.String(args[4])); err != nil {
				return ValueErr
			}

			if count, err = strconv.Atoi(unsafe2.String(args[5])); err != nil {
				return ValueErr
			}
		}

		key := args[0]
		if ay, err := proxyClient.ZRangeByLex(key, min, max, rangeType, offset, count); err != nil {
			return err
		} else {
			s.RespWriter.WriteSliceArray(ay)
		}

		return nil
	}
*/
func ZremrangebylexCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.ZREMRANGEBYLEX)
	}

	min, max, _, err := zparseMemberRange(args[1], args[2])
	if err != nil {
		return err
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		key := unsafe2.String(args[0])
		res, err := proxyClient.ZRemRangeByLex(s, key, unsafe2.String(min), unsafe2.String(max))
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if n, err := redis.Int64(res, err); err != nil && err != redis.ErrNil {
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

func ZlexcountCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.ZLEXCOUNT)
	}

	min, max, _, err := zparseMemberRange(args[1], args[2])
	if err != nil {
		return err
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		key := unsafe2.String(args[0])
		res, err := proxyClient.ZLexCount(s, key, unsafe2.String(min), unsafe2.String(max))
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if n, err := redis.Int64(res, err); err != nil && err != redis.ErrNil {
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

func ZClearCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 1 {
		return resp.CmdParamsErr(resp.ZCLEAR)
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.ZClear(s, args[:]...)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if n, err := redis.Int64(res, err); err != nil && err != redis.ErrNil {
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

func ZExpireatCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.ZEXPIREAT)
	}
	when, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}
	res, err := proxyClient.ZExpireAt(s, args[0], when)
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

func ZPersistCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.ZPERSIST)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.ZPersist(s, args[0])
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

func ZKeyExistsCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.ZKEYEXISTS)
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.ZKeyExists(s, args[0])
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

func ZTtlCommand(s *resp.Session) error {
	args := s.Args

	if len(args) != 1 {
		return resp.CmdParamsErr(resp.ZTTL)
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.ZTtl(s, args[0])
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

func ZExpireCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.ZEXPIRE)
	}

	duration, err := extend.ParseInt64(unsafe2.String(args[1]))
	if err != nil {
		return resp.ValueErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		res, err := proxyClient.ZExpire(s, args[0], duration)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			if v, err2 := resp.Int64(redis.Bool(res, err)); err2 != nil {
				return err2
			} else {
				s.RespWriter.WriteInteger(v)
			}
		}
	} else {
		return err
	}

	return nil
}

func ZRangeByLexCommand(s *resp.Session) error {
	args := s.Args
	if len(args) != 3 && len(args) != 6 {
		return resp.CmdParamsErr(resp.ZRANGEBYLEX)
	}

	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}

	output := func(s *resp.Session, res interface{}, err error) (retErr error) {
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		} else {
			data, err := redis.ByteSlices(res, err)
			if err != nil && err != redis.ErrNil {
				return err
			}
			s.RespWriter.WriteSliceArray(data)
		}
		return nil
	}

	if len(args) != 6 {
		res, err := proxyClient.ZRangeByLex(s, unsafe2.String(args[0]), unsafe2.String(args[1]), unsafe2.String(args[2]))
		return output(s, res, err)
	}

	var offset int = 0
	var count int = -1
	if strings.ToLower(unsafe2.String(args[3])) != "limit" {
		return resp.SyntaxErr
	}
	if offset, err = strconv.Atoi(unsafe2.String(args[4])); err != nil {
		return resp.ValueErr
	}
	if count, err = strconv.Atoi(unsafe2.String(args[5])); err != nil {
		return resp.ValueErr
	}
	res, err := proxyClient.ZRangeByLex(s, unsafe2.String(args[0]), unsafe2.String(args[1]), unsafe2.String(args[2]), "limit", offset, count)
	return output(s, res, err)
}
