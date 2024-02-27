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

package cmd_test

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

var cnt = 1024
var srcAddr = ""
var destAddr = ""

func TestMockData(t *testing.T) {
	if srcAddr == "" || destAddr == "" {
		return
	}
	if err := setData(srcAddr); err != nil {
		t.Fatal(err)
	}
	if err := getData(srcAddr); err != nil {
		t.Fatal(err)
	}
}

func TestCheckData(t *testing.T) {
	if destAddr == "" {
		return
	}
	if err := getData(destAddr); err != nil {
		t.Fatal(err)
	}
}

func getData(addr string) error {
	c, err := getConn(addr)
	if err == nil && c != nil {
		defer c.Close()
	} else {
		return err
	}
	for i := 0; i < cnt; i++ {
		if n, err := redis.Int(c.Do("exists", "key::"+strconv.Itoa(i))); err != nil || n != 1 {
			return errors.New("migrate error" + "key::" + strconv.Itoa(i))
		}
		if n, err := redis.Int(c.Do("exists", "hkey::"+strconv.Itoa(i))); err != nil || n != 1 {
			return errors.New("migrate error" + "key::" + strconv.Itoa(i))
		}
		if n, err := redis.Int(c.Do("exists", "lkey::"+strconv.Itoa(i))); err != nil || n != 1 {
			return errors.New("migrate error" + "key::" + strconv.Itoa(i))
		}
		if n, err := redis.Int(c.Do("exists", "skey::"+strconv.Itoa(i))); err != nil || n != 1 {
			return errors.New("migrate error" + "key::" + strconv.Itoa(i))
		}
		if n, err := redis.Int(c.Do("exists", "zkey::"+strconv.Itoa(i))); err != nil || n != 1 {
			return errors.New("migrate error" + "key::" + strconv.Itoa(i))
		}
	}
	return nil
}

func setData(addr string) error {
	c, err := getConn(addr)
	if err == nil && c != nil {
		defer c.Close()
	} else {
		return err
	}
	for i := 0; i < cnt; i++ {
		if _, err := c.Do("set", "key::"+strconv.Itoa(i), i); err != nil {
			return err
		}
		if _, err := c.Do("hset", "hkey::"+strconv.Itoa(i), i, i); err != nil {
			return err
		}
		if _, err := c.Do("lpush", "lkey::"+strconv.Itoa(i), i); err != nil {
			return err
		}
		if _, err := c.Do("sadd", "skey::"+strconv.Itoa(i), i); err != nil {
			return err
		}
		if _, err := c.Do("zadd", "zkey::"+strconv.Itoa(i), i, i); err != nil {
			return err
		}
	}
	return nil
}

func TestMockLuaData(t *testing.T) {
	if srcAddr == "" || destAddr == "" {
		return
	}
	if err := setLuaData(srcAddr); err != nil {
		t.Fatal(err)
	}
	if err := getLuaData(srcAddr); err != nil {
		t.Fatal(err)
	}
}

func TestCheckLuaData(t *testing.T) {
	if destAddr == "" {
		return
	}
	if err := getLuaData(destAddr); err != nil {
		t.Fatal(err)
	}
}

func TestLua(t *testing.T) {
	if destAddr == "" {
		return
	}
	if err := setLuaData(destAddr); err != nil {
		t.Fatal(err)
	}
}

func getLuaData(addr string) error {
	c, err := getConn(addr)
	if err == nil && c != nil {
		defer c.Close()
	} else {
		return err
	}
	luaScript := "return redis.call(KEYS[1], KEYS[2])"
	for i := 0; i < cnt; i++ {
		if n, err := redis.Int(c.Do("eval", luaScript, 2, "exists", "{test}key::"+strconv.Itoa(i))); err != nil || n != 1 {
			return errors.New("migrate error" + "key::" + strconv.Itoa(i))
		}
		if n, err := redis.Int(c.Do("eval", luaScript, 2, "exists", "{test}hkey::"+strconv.Itoa(i))); err != nil || n != 1 {
			return errors.New("migrate error" + "key::" + strconv.Itoa(i))
		}
		if n, err := redis.Int(c.Do("eval", luaScript, 2, "exists", "{test}lkey::"+strconv.Itoa(i))); err != nil || n != 1 {
			return errors.New("migrate error" + "key::" + strconv.Itoa(i))
		}
		if n, err := redis.Int(c.Do("eval", luaScript, 2, "exists", "{test}skey::"+strconv.Itoa(i))); err != nil || n != 1 {
			return errors.New("migrate error" + "key::" + strconv.Itoa(i))
		}
		if n, err := redis.Int(c.Do("eval", luaScript, 2, "exists", "{test}zkey::"+strconv.Itoa(i))); err != nil || n != 1 {
			return errors.New("migrate error" + "key::" + strconv.Itoa(i))
		}
	}
	return nil
}

func setLuaData(addr string) error {
	c, err := getConn(addr)
	if err == nil && c != nil {
		defer c.Close()
	} else {
		return err
	}
	luaScript := "return redis.call(KEYS)"
	var args []interface{}
	for i := 0; i < cnt; i++ {
		args = []interface{}{luaScript, 3, "set", "{test}key::" + strconv.Itoa(i), i}
		if _, err := c.Do("eval", args...); err != nil {
			return err
		}
		args = []interface{}{luaScript, 4, "hset", "{test}hkey::" + strconv.Itoa(i), i, i}
		if _, err := c.Do("eval", args...); err != nil {
			return err
		}
		args = []interface{}{luaScript, 3, "lpush", "{test}lkey::" + strconv.Itoa(i), i}
		if _, err := c.Do("eval", args...); err != nil {
			return err
		}
		args = []interface{}{luaScript, 3, "sadd", "{test}skey::" + strconv.Itoa(i), i}
		if _, err := c.Do("eval", args...); err != nil {
			return err
		}
		args = []interface{}{luaScript, 4, "zadd", "{test}zkey::" + strconv.Itoa(i), i, i}
		if _, err := c.Do("eval", args...); err != nil {
			return err
		}
	}
	return nil
}

func getConn(addr string) (conn redis.Conn, e error) {
	conn, err := redis.Dial("tcp", addr,
		redis.DialPassword(""),
		redis.DialDatabase(0),
		redis.DialConnectTimeout(1*time.Second),
		redis.DialReadTimeout(1*time.Second),
		redis.DialWriteTimeout(1*time.Second),
	)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
