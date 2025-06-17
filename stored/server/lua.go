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
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"

	lua "github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/luajson"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

var luaClientPool sync.Pool
var LuaCompileCacheCount int16 = 2000
var ProtectGlobalsLua = "protectGlobals"

type LuaClient struct {
	LState *lua.LState
	Count  int16
}

type CompiledLuaScripts struct {
	Scripts map[string]*lua.FunctionProto
	Count   int16
	Lock    sync.RWMutex
}

func NewCompiledLuaScripts() *CompiledLuaScripts {
	cs := &CompiledLuaScripts{
		Scripts: make(map[string]*lua.FunctionProto, LuaCompileCacheCount),
	}
	proto, _ := CompileLua(protectGlobals)
	cs.Scripts[ProtectGlobalsLua] = proto
	return cs
}

func ClearCompiledLuaScripts(s *Server) {
	s.luaScripts.Lock.Lock()
	s.luaScripts.Scripts = make(map[string]*lua.FunctionProto, LuaCompileCacheCount)
	proto, _ := CompileLua(protectGlobals)
	s.luaScripts.Scripts[ProtectGlobalsLua] = proto
	s.luaScripts.Count = 0
	s.luaScripts.Lock.Unlock()
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
			proto, _ := LoadOrCompileLua(s, ProtectGlobalsLua, protectGlobals)
			_ = DoCompiledScript(l, proto)
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

func TryGetLuaProto(s *Server, sha1 string) *lua.FunctionProto {
	s.luaScripts.Lock.RLock()
	if proto, exists := s.luaScripts.Scripts[sha1]; exists {
		s.luaScripts.Lock.RUnlock()
		return proto
	}
	s.luaScripts.Lock.RUnlock()
	return nil
}

func LoadLuaProto(s *Server, sha1 string, proto *lua.FunctionProto) {
	s.luaScripts.Lock.Lock()
	defer s.luaScripts.Lock.Unlock()
	if _, exists := s.luaScripts.Scripts[sha1]; exists {
		return
	}
	if s.luaScripts.Count >= LuaCompileCacheCount {
		for k, _ := range s.luaScripts.Scripts {
			if k == ProtectGlobalsLua {
				continue
			}
			delete(s.luaScripts.Scripts, k)
			s.luaScripts.Count--
			log.Infof("lua compile cache count reached %d  delete one %s", LuaCompileCacheCount, k)
			break
		}
	}
	s.luaScripts.Scripts[sha1] = proto
	s.luaScripts.Count++
}

func LoadOrCompileLua(s *Server, sha1 string, script string) (*lua.FunctionProto, error) {
	proto := TryGetLuaProto(s, sha1)
	if proto != nil {
		return proto, nil
	}
	proto, err := CompileLua(script)
	if err != nil {
		log.Errorf("ERR Error compiling script (new function): %s", err.Error())
		return nil, errors.New(ErrLuaParseError(err))
	}
	LoadLuaProto(s, sha1, proto)
	return proto, nil
}

func CompileLua(script string) (*lua.FunctionProto, error) {
	reader := strings.NewReader(script)
	chunk, err := parse.Parse(reader, "<script>")
	if err != nil {
		return nil, err
	}
	proto, err := lua.Compile(chunk, "<script>")
	if err != nil {
		return nil, err
	}
	return proto, nil
}

func DoCompiledScript(L *lua.LState, proto *lua.FunctionProto) error {
	lfunc := L.NewFunctionFromProto(proto)
	L.Push(lfunc)
	return L.PCall(0, lua.MultRet, nil)
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
			_ = vmClient.HandleRequest(reqData, true)
			buf := bytes.NewBuffer(vmClient.Writer.Bytes())
			defer vmClient.Writer.Reset()
			res, err := ParseReply(bufio.NewReader(buf))
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
		c.Writer.WriteBulk(nil)
		return
	}

	switch t := value.(type) {
	case *lua.LNilType:
		c.Writer.WriteBulk(nil)
	case lua.LBool:
		if lua.LVAsBool(value) {
			c.Writer.WriteInteger(1)
		} else {
			c.Writer.WriteBulk(nil)
		}
	case lua.LNumber:
		c.Writer.WriteInteger(int64(lua.LVAsNumber(value)))
	case lua.LString:
		s := lua.LVAsString(value)
		c.Writer.WriteBulk([]byte(s))
	case *lua.LTable:
		if s := t.RawGetString("err"); s.Type() != lua.LTNil {
			c.Writer.WriteError(errors.New(s.String()))
			return
		}
		if s := t.RawGetString("ok"); s.Type() != lua.LTNil {
			c.Writer.WriteStatus(s.String())
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

		c.Writer.WriteLen(len(result))
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
