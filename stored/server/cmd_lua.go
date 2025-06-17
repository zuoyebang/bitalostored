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

package server

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	lua "github.com/yuin/gopher-lua"
	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

var LuaShardCount uint32 = 64

const (
	MsgWrongType          = "WRONGTYPE Operation against a key holding the wrong kind of value"
	MsgInvalidInt         = "ERR value is not an integer or out of range"
	MsgInvalidFloat       = "ERR value is not a valid float"
	MsgInvalidMinMax      = "ERR min or max is not a float"
	MsgInvalidRangeItem   = "ERR min or max not valid string range item"
	MsgInvalidTimeout     = "ERR timeout is not a float or out of range"
	MsgErrSyntaxor        = "ERR syntax error"
	MsgKeyNotFound        = "ERR no such key"
	MsgOutOfRange         = "ERR index out of range"
	MsgInvalidCursor      = "ERR invalid cursor"
	MsgXXandNX            = "ERR XX and NX options at the same time are not compatible"
	MsgNegTimeout         = "ERR timeout is negative"
	MsgInvalidSETime      = "ERR invalid expire time in set"
	MsgInvalidSETEXTime   = "ERR invalid expire time in setex"
	MsgInvalidPSETEXTime  = "ERR invalid expire time in psetex"
	MsgInvalidKeysNumber  = "ERR Number of keys can't be greater than number of args"
	MsgNegativeKeysNumber = "ERR Number of keys can't be negative"
	MsgFScriptUsage       = "ERR Unknown subcommand or wrong number of arguments for '%s'. Try SCRIPT HELP."
	MsgFPubsubUsage       = "ERR Unknown subcommand or wrong number of arguments for '%s'. Try PUBSUB HELP."
	MsgSingleElementPair  = "ERR INCR option supports a single increment-element pair"
	MsgInvalidStreamID    = "ERR Invalid stream ID specified as stream command argument"
	MsgStreamIDTooSmall   = "ERR The ID specified in XADD is equal or smaller than the target stream top item"
	MsgStreamIDZero       = "ERR The ID specified in XADD must be greater than 0-0"
	MsgNoScriptFound      = "NOSCRIPT No matching script. Please use EVAL."
	MsgUnsupportedUnit    = "ERR unsupported unit provided. please use m, km, ft, mi"
	MsgNotFromScripts     = "This Redis command is not allowed from scripts"
	MsgXreadUnbalanced    = "ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified."
)

func ErrWrongNumber(cmd string) string {
	return fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(cmd))
}

func ErrLuaParseError(err error) string {
	return fmt.Sprintf("ERR Error compiling script (new function):L %s", strings.Replace(err.Error(), "\n", "", -1))
}

func init() {
	AddCommand(map[string]*Cmd{
		resp.EVAL:         {Sync: resp.IsWriteCmd(resp.EVAL), Handler: evalCommand, NotAllowedInTx: true},
		resp.EVALSHA:      {Sync: resp.IsWriteCmd(resp.EVALSHA), Handler: evalShaCommand, NotAllowedInTx: true},
		resp.SCRIPTLOAD:   {Sync: resp.IsWriteCmd(resp.SCRIPTLOAD), Handler: scriptLoadCmd, NotAllowedInTx: true},
		resp.SCRIPTFLUSH:  {Sync: resp.IsWriteCmd(resp.SCRIPTFLUSH), Handler: scriptFlushCmd, NotAllowedInTx: true},
		resp.SCRIPTEXISTS: {Sync: resp.IsWriteCmd(resp.SCRIPTEXISTS), Handler: scriptExistsCmd, NotAllowedInTx: true},
		resp.SCRIPTLEN:    {Sync: resp.IsWriteCmd(resp.SCRIPTLEN), Handler: scriptLenCmd, NotAllowedInTx: true},
	})
}

func evalCommand(c *Client) error {
	args := StringSlice(c.Args)
	if len(args) < 2 {
		return errn.CmdParamsErr(resp.EVAL)
	}

	script, args := args[0], args[1:]
	keys, args, err := checkArgs(args)
	if err != nil {
		return err
	}
	sha1 := utils.Sha1Hex(script)
	proto, err := LoadOrCompileLua(c.server, sha1, script)
	if err != nil {
		return err
	}
	err = runLuaScript(c, proto, keys, args)
	if err == nil {
		_, _ = saveLuaScript(c, script, sha1)
	}
	return err
}

func checkArgs(args []string) ([]string, []string, error) {
	keysS, args := args[0], args[1:]
	keysLen, err := strconv.Atoi(keysS)

	if err != nil {
		return nil, nil, errors.New(MsgInvalidInt)
	}

	if keysLen < 0 {
		return nil, nil, errors.New(MsgNegativeKeysNumber)
	}
	if keysLen > len(args) {
		return nil, nil, errors.New(MsgInvalidKeysNumber)
	}
	keys, args := args[:keysLen], args[keysLen:]
	return keys, args, nil
}

func runLuaScript(c *Client, proto *lua.FunctionProto, keys, args []string) error {
	if len(keys) > 0 {
		shard := c.KeyHash % LuaShardCount
		c.server.luaMu[shard].Lock()
		defer c.server.luaMu[shard].Unlock()
	}

	l := GetLuaClientFromPool()
	luaClient := l.LState
	defer PutLuaClientToPool(l)

	keysTable := luaClient.NewTable()
	for i, k := range keys {
		luaClient.RawSet(keysTable, lua.LNumber(i+1), lua.LString(k))
	}
	luaClient.SetGlobal("KEYS", keysTable)

	argvTable := luaClient.NewTable()
	for i, a := range args {
		luaClient.RawSet(argvTable, lua.LNumber(i+1), lua.LString(a))
	}
	luaClient.SetGlobal("ARGV", argvTable)

	defer func() {
		luaClient.Remove(1)
		luaClient.Remove(1)
		luaClient.Remove(lua.GlobalsIndex)
	}()
	err := DoCompiledScript(luaClient, proto)
	if err != nil {
		log.Errorf("ERR Error running script: %s", err.Error())
		return errors.New(ErrLuaParseError(err))
	}
	LuaToRedis(luaClient, c, luaClient.Get(1))
	return nil
}

func evalShaCommand(c *Client) error {
	args := StringSlice(c.Args)
	if len(args) < 2 {
		return errn.CmdParamsErr(resp.EVALSHA)
	}

	sha1, args := args[0], args[1:]
	keys, args, err := checkArgs(args)
	if err != nil {
		c.Writer.WriteError(err)
		return nil
	}

	proto := TryGetLuaProto(c.server, sha1)
	if proto == nil {
		script, closer := c.DB.GetLuaScript([]byte(sha1))
		defer func() {
			if closer != nil {
				closer()
			}
		}()
		if script == nil {
			c.Writer.WriteError(errors.New(MsgNoScriptFound))
			return nil
		}
		proto, err = CompileLua(string(script))
		if err != nil {
			c.Writer.WriteError(err)
			return nil
		}
		LoadLuaProto(c.server, sha1, proto)
	}
	if err := runLuaScript(c, proto, keys, args); err != nil {
		c.Writer.WriteError(err)
	}

	return nil
}

func scriptLoadCmd(c *Client) error {
	args := StringSlice(c.Args)
	if len(args) != 2 {
		return errors.New(fmt.Sprintf(MsgFScriptUsage, "LOAD"))
	}
	script := args[1]
	var err error
	sha1 := utils.Sha1Hex(script)
	_, err = LoadOrCompileLua(c.server, sha1, script)
	if err != nil {
		return err
	}
	if sha1, err = saveLuaScript(c, script, sha1); err != nil {
		return err
	}
	c.Writer.WriteBulk([]byte(sha1))
	return nil
}

func saveLuaScript(c *Client, script, sha1 string) (string, error) {
	if n, _ := c.DB.ExistsLuaScript([]byte(sha1)); n >= 1 {
		return sha1, nil
	}

	if err := c.DB.SetLuaScript([]byte(sha1), []byte(script)); err != nil {
		return "", err
	}
	return sha1, nil
}

func scriptExistsCmd(c *Client) error {
	args := StringSlice(c.Args)
	if len(args) < 2 {
		return errors.New(fmt.Sprintf(MsgFScriptUsage, "EXISTS"))
	}

	args = args[1:]
	c.Writer.WriteLen(len(args))
	for _, arg := range args {
		if n, _ := c.DB.ExistsLuaScript([]byte(arg)); n >= 1 {
			c.Writer.WriteInteger(1)
		} else {
			c.Writer.WriteInteger(0)
		}
	}
	return nil
}

func scriptFlushCmd(c *Client) error {
	args := StringSlice(c.Args)
	if len(args) != 1 {
		return errors.New(fmt.Sprintf(MsgFScriptUsage, "FLUSH"))
	}

	if err := c.DB.FlushLuaScript(); err != nil {
		return err
	} else {
		ClearCompiledLuaScripts(c.server)
		c.Writer.WriteStatus("OK")
	}
	return nil
}

func scriptLenCmd(c *Client) error {
	n := c.DB.LuaScriptLen()
	c.server.luaScripts.Lock.RLock()
	m := c.server.luaScripts.Count
	l := len(c.server.luaScripts.Scripts)
	c.server.luaScripts.Lock.RUnlock()
	ay := []interface{}{
		extend.FormatInt64ToSlice(n),
		extend.FormatInt16ToSlice(m),
		extend.FormatIntToSlice(l),
	}

	c.Writer.WriteArray(ay)
	return nil
}
