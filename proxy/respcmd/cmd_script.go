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
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/proxy/resp"
	"github.com/zuoyebang/bitalostored/proxy/router"

	"github.com/gomodule/redigo/redis"
)

func init() {
	resp.Register(resp.EVAL, EvalCommand)
	resp.Register(resp.EVALSHA, EvalshaCommand)
	resp.Register(resp.SCRIPT, ScriptCommand)
}

func EvalCommand(s *resp.Session) error {
	return evalCommon(s, resp.EVAL)
}

func evalCommon(s *resp.Session, command string) error {
	args := s.Args
	argsLength := len(args)
	if argsLength < 3 {
		return resp.CmdParamsErr(command)
	}

	keyNumStr := string(args[1])
	keyNum, err := strconv.Atoi(keyNumStr)
	if err != nil || keyNum <= 0 {
		return resp.ValueErr
	}
	if len(args) < keyNum+2 {
		return resp.CmdParamsErr(command)
	}

	if !keysHasSameTag(args[2 : 2+keyNum]) {
		return resp.HashTagErr
	}

	script := unsafe2.String(args[0])
	key1 := unsafe2.String(args[2])
	argsInterface := make([]interface{}, 0, argsLength)
	argsInterface = append(argsInterface, script)
	argsInterface = append(argsInterface, keyNum)
	argsInterface = append(argsInterface, key1)
	if argsLength > 3 {
		for _, arg := range args[3:] {
			argsInterface = append(argsInterface, arg)
		}
	}

	if proxyClient, err := router.GetProxyClient(); err == nil {
		reply, err := proxyClient.EvalCommon(command, s, argsInterface...)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		}
		if err != nil {
			return err
		}
		switch reply := reply.(type) {
		case []interface{}:
			s.RespWriter.WriteArray(reply)
		case []byte:
			s.RespWriter.WriteBulk(reply)
		case nil:
			s.RespWriter.WriteBulk(nil)
		case int64:
			s.RespWriter.WriteInteger(reply)
		case string:
			s.RespWriter.WriteStatus(reply)
		case error:
			s.RespWriter.WriteError(reply)
		default:
			return fmt.Errorf("lua response: unexpected type for %s, got type %T", command, reply)
		}
	} else {
		return err
	}

	return nil
}

func keysHasSameTag(keys [][]byte) bool {
	if len(keys) == 0 {
		return false
	}
	firstTag := resp.ExtractHashTag(string(keys[0]))
	for i := 1; i < len(keys); i++ {
		tag := resp.ExtractHashTag(string(keys[i]))
		if bytes.Compare(tag, firstTag) != 0 {
			return false
		}
	}
	return true
}

func EvalshaCommand(s *resp.Session) error {
	return evalCommon(s, resp.EVALSHA)
}

func ScriptCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 1 {
		return resp.CmdParamsErr(resp.SCRIPT)
	}
	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}

	switch strings.ToUpper(unsafe2.String(args[0])) {
	case "LOAD":
		if len(s.Args) != 2 {
			return resp.CmdParamsErr(resp.SCRIPT)
		}
		v, err := proxyClient.ScriptLoad(s, unsafe2.String(args[1]))
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		}
		res, err2 := redis.Bytes(v, err)
		if err2 != nil {
			return err2
		}
		s.RespWriter.WriteBulk(res)
	case "EXISTS":
		var argsInterface []interface{}
		scriptArgs := resp.StringSlice(args)
		for _, arg := range scriptArgs {
			argsInterface = append(argsInterface, arg)
		}
		v, err := proxyClient.ScriptExists(resp.SCRIPT, s, argsInterface...)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		}
		res, err2 := redis.Values(v, err)
		if err2 != nil {
			return err2
		}
		s.RespWriter.WriteArray(res)
	case "FLUSH":
		_, err := proxyClient.ScriptFlush(s)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		}
		if err != nil {
			return err
		}
		s.RespWriter.WriteStatus(resp.ReplyOK)
	case "LEN":
		v, err := proxyClient.ScriptLen(s)
		if s.TxCommandQueued {
			return s.SendTxQueued(err)
		}
		res, err2 := redis.Int64(v, err)
		if err2 != nil {
			return err2
		}
		s.RespWriter.WriteInteger(res)
	default:
		return resp.NotImplementErr
	}

	return nil
}
