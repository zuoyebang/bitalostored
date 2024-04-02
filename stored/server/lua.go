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

package server

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/luajson"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"

	lua "github.com/yuin/gopher-lua"
)

var luaClientPool sync.Pool

type LuaClient struct {
	LState *lua.LState
	Count  int16
}

func InitLuaPool(s *Server) {
	luaClientPool = sync.Pool{
		New: func() interface{} {
			l := lua.NewState(lua.Options{SkipOpenLibs: true})
			for _, pair := range []struct {
				n string
				f lua.LGFunction
			}{
				{lua.LoadLibName, lua.OpenPackage},
				{lua.BaseLibName, lua.OpenBase},
				{lua.CoroutineLibName, lua.OpenCoroutine},
				{lua.TabLibName, lua.OpenTable},
				{lua.StringLibName, lua.OpenString},
				{lua.MathLibName, lua.OpenMath},
				{lua.DebugLibName, lua.OpenDebug},
			} {
				if err := l.CallByParam(lua.P{
					Fn:      l.NewFunction(pair.f),
					NRet:    0,
					Protect: true,
				}, lua.LString(pair.n)); err != nil {
					panic(err)
				}
			}
			luajson.Preload(l)
			requireGlobal(l, "cjson", "json")
			l.PreloadModule("redis", redisLoader(s))
			requireGlobal(l, "redis", "redis")
			_ = l.DoString(protectGlobals)
			return &LuaClient{l, 0}
		},
	}
	for i := 0; i < 16; i++ {
		luaClientPool.Put(luaClientPool.New())
	}
}

func GetLuaClientFromPool() *LuaClient {
	l := luaClientPool.Get().(*LuaClient)
	l.Count++
	return l
}

func PutLuaClientToPool(l *LuaClient) {
	if l.Count > 10000 {
		l.LState.Close()
		l = luaClientPool.New().(*LuaClient)
	}
	luaClientPool.Put(l)
}

func MkLuaFuncs(srv *Server) map[string]lua.LGFunction {
	mkCall := func(failFast bool) func(l *lua.LState) int {
		return func(l *lua.LState) int {
			top := l.GetTop()
			if top == 0 {
				l.Error(lua.LString("Please specify at least one argument for redis.call()"), 0)
				return 0
			}
			var args []string
			for i := 1; i <= top; i++ {
				switch a := l.Get(i).(type) {
				case lua.LNumber:
					args = append(args, a.String())
				case lua.LString:
					args = append(args, string(a))
				case *lua.LTable:
					tableArgs := ConvertLuaTable(l, a)
					args = append(args, tableArgs...)
				default:
					l.Error(lua.LString("Lua redis() command arguments must be strings or integers"), 0)
					return 0
				}
			}
			if len(args) == 0 {
				l.Error(lua.LString(MsgNotFromScripts), 0)
				return 0
			}

			reqData := utils.StringSliceToByteSlice(args)
			vmClient := GetVmFromPool(srv)
			defer PutRaftClientToPool(vmClient)
			isPlugin := config.GlobalConfig.Plugin.OpenRaft
			_ = vmClient.HandleRequest(isPlugin, reqData, true)
			buf := bytes.NewBuffer(vmClient.RespWriter.FlushToBytes())
			res, err := resp.ParseReply(bufio.NewReader(buf))
			if err != nil {
				if failFast {
					if strings.Contains(err.Error(), "empty command") {
						l.Error(lua.LString("Unknown Redis command called from Lua script"), 0)
					} else {
						l.Error(lua.LString(err.Error()), 0)
					}
					return 0
				}

				l.Push(lua.LNil)
				return 1
			}

			if res == nil {
				l.Push(lua.LFalse)
			} else {
				switch r := res.(type) {
				case int64:
					l.Push(lua.LNumber(r))
				case int:
					l.Push(lua.LNumber(r))
				case []uint8:
					l.Push(lua.LString(r))
				case []interface{}:
					l.Push(redisToLua(l, r))
				case string:
					l.Push(lua.LString(r))
				case error:
					l.Error(lua.LString(r.Error()), 0)
					return 0
				default:
					panic(fmt.Sprintf("type not handled (%T)", r))
				}
			}
			return 1
		}
	}

	return map[string]lua.LGFunction{
		"call":  mkCall(true),
		"pcall": mkCall(false),
		"error_reply": func(l *lua.LState) int {
			msg := l.CheckString(1)
			res := &lua.LTable{}
			res.RawSetString("err", lua.LString(msg))
			l.Push(res)
			return 1
		},
		"status_reply": func(l *lua.LState) int {
			msg := l.CheckString(1)
			res := &lua.LTable{}
			res.RawSetString("ok", lua.LString(msg))
			l.Push(res)
			return 1
		},
		"sha1hex": func(l *lua.LState) int {
			top := l.GetTop()
			if top != 1 {
				l.Error(lua.LString("wrong number of arguments"), 0)
				return 0
			}
			msg := lua.LVAsString(l.Get(1))
			l.Push(lua.LString(utils.Sha1Hex(msg)))
			return 1
		},
		"replicate_commands": func(l *lua.LState) int {
			return 1
		},
	}
}

func ConvertLuaTable(l *lua.LState, value lua.LValue) []string {
	var tableVal []lua.LValue
	for j := 1; true; j++ {
		val := l.GetTable(value, lua.LNumber(j))
		if val == nil {
			tableVal = append(tableVal, val)
			continue
		}
		if val.Type() == lua.LTNil {
			break
		}
		tableVal = append(tableVal, val)
	}
	var result []string
	for _, r := range tableVal {
		switch t := r.(type) {
		case lua.LNumber:
			result = append(result, t.String())
		case lua.LString:
			result = append(result, string(t))
		default:
			l.Error(lua.LString("Unknown table val"), 0)
		}
	}
	return result
}

func LuaToRedis(l *lua.LState, c *Client, value lua.LValue) {
	if value == nil {
		c.RespWriter.WriteBulk(nil)
		return
	}

	switch t := value.(type) {
	case *lua.LNilType:
		c.RespWriter.WriteBulk(nil)
	case lua.LBool:
		if lua.LVAsBool(value) {
			c.RespWriter.WriteInteger(1)
		} else {
			c.RespWriter.WriteBulk(nil)
		}
	case lua.LNumber:
		c.RespWriter.WriteInteger(int64(lua.LVAsNumber(value)))
	case lua.LString:
		s := lua.LVAsString(value)
		c.RespWriter.WriteBulk([]byte(s))
	case *lua.LTable:
		if s := t.RawGetString("err"); s.Type() != lua.LTNil {
			c.RespWriter.WriteError(errors.New(s.String()))
			return
		}
		if s := t.RawGetString("ok"); s.Type() != lua.LTNil {
			c.RespWriter.WriteStatus(s.String())
			return
		}

		var result []lua.LValue
		for j := 1; true; j++ {
			val := l.GetTable(value, lua.LNumber(j))
			if val == nil {
				result = append(result, val)
				continue
			}

			if val.Type() == lua.LTNil {
				break
			}

			result = append(result, val)
		}

		c.RespWriter.WriteLen(len(result))
		for _, r := range result {
			LuaToRedis(l, c, r)
		}
	default:
		panic("....")
	}
}

func redisToLua(l *lua.LState, res []interface{}) *lua.LTable {
	rettb := l.NewTable()
	for _, e := range res {
		var v lua.LValue
		if e == nil {
			v = lua.LFalse
		} else {
			switch et := e.(type) {
			case int64:
				v = lua.LNumber(et)
			case []uint8:
				v = lua.LString(string(et))
			case []interface{}:
				v = redisToLua(l, et)
			case string:
				v = lua.LString(et)
			default:
				v = lua.LString(e.(string))
			}
		}
		l.RawSet(rettb, lua.LNumber(rettb.Len()+1), v)
	}
	return rettb
}

func requireGlobal(l *lua.LState, id, modName string) {
	if err := l.CallByParam(lua.P{
		Fn:      l.GetGlobal("require"),
		NRet:    1,
		Protect: true,
	}, lua.LString(modName)); err != nil {
		panic(err)
	}
	mod := l.Get(-1)
	l.Pop(1)

	l.SetGlobal(id, mod)
}

func redisLoader(srv *Server) func(L *lua.LState) int {
	return func(L *lua.LState) int {
		t := L.NewTable()
		L.SetFuncs(t, MkLuaFuncs(srv))
		L.Push(t)
		return 1
	}
}

var protectGlobals = `
local dbg=debug
local mt = {}
setmetatable(_G, mt)
mt.__newindex = function (t, n, v)
  if dbg.getinfo(2) then
    local w = dbg.getinfo(2, "S").what
    if w ~= "C" then
      error("Script attempted to create global variable '"..tostring(n).."'", 2)
    end
  end
  rawset(t, n, v)
end
mt.__index = function (t, n)
  if dbg.getinfo(2) and dbg.getinfo(2, "S").what ~= "C" then
    error("Script attempted to access nonexistent global variable '"..tostring(n).."'", 2)
  end
  return rawget(t, n)
end
debug = nil

`
