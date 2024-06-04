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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/zuoyebang/bitalostored/proxy/resp"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func testListIndex(key []byte, index int64, v int) error {
	c := getTestConn()
	defer c.Close()

	n, err := redis.Int(c.Do("lindex", key, index))
	if err == redis.ErrNil && v != 0 {
		return fmt.Errorf("must nil")
	} else if err != nil && err != redis.ErrNil {
		return err
	} else if n != v {
		return fmt.Errorf("index err number %d != %d", n, v)
	}

	return nil
}

func testListRange(key []byte, start int64, stop int64, checkValues ...int) error {
	c := getTestConn()
	defer c.Close()

	vs, err := redis.Values(c.Do("lrange", key, start, stop))
	if err != nil {
		return err
	}

	if len(vs) != len(checkValues) {
		return fmt.Errorf("invalid return number %d != %d", len(vs), len(checkValues))
	}

	var n int
	for i, v := range vs {
		if d, ok := v.([]byte); ok {
			n, err = strconv.Atoi(string(d))
			if err != nil {
				return err
			} else if n != checkValues[i] {
				return fmt.Errorf("invalid data %d: %d != %d", i, n, checkValues[i])
			}
		} else {
			return fmt.Errorf("invalid data %v %T", v, v)
		}
	}

	return nil
}

func TestList(t *testing.T) {
	c := getTestConn()
	defer c.Close()
	key := []byte("TESTLIST-Proxy-111")
	c.Do("del", key)

	n, err := redis.Int(c.Do("lpush", key, 1))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	n, err = redis.Int(c.Do("rpush", key, 2))
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	n, err = redis.Int(c.Do("rpush", key, 3))
	assert.NoError(t, err)
	assert.Equal(t, 3, n)

	time.Sleep(30 * time.Millisecond)
	n, err = redis.Int(c.Do("llen", key))
	assert.NoError(t, err)
	assert.Equal(t, 3, n)

	//for ledis-cli a 1 2 3
	// 127.0.0.1:6379> lrange a 0 0
	// 1) "1"
	err = testListRange(key, 0, 0, 1)
	assert.NoError(t, err)

	// 127.0.0.1:6379> lrange a 0 1
	// 1) "1"
	// 2) "2"
	err = testListRange(key, 0, 1, 1, 2)
	assert.NoError(t, err)

	// 127.0.0.1:6379> lrange a 0 5
	// 1) "1"
	// 2) "2"
	// 3) "3"
	err = testListRange(key, 0, 5, 1, 2, 3)
	assert.NoError(t, err)

	// 127.0.0.1:6379> lrange a -1 5
	// 1) "3"
	err = testListRange(key, -1, 5, 3)
	assert.NoError(t, err)

	// 127.0.0.1:6379> lrange a -5 -1
	// 1) "1"
	// 2) "2"
	// 3) "3"
	err = testListRange(key, -5, -1, 1, 2, 3)
	assert.NoError(t, err)

	// 127.0.0.1:6379> lrange a -2 -1
	// 1) "2"
	// 2) "3"
	err = testListRange(key, -2, -1, 2, 3)
	assert.NoError(t, err)

	// 127.0.0.1:6379> lrange a -1 -2
	// (empty list or set)
	err = testListRange(key, -1, -2)
	assert.NoError(t, err)

	// 127.0.0.1:6379> lrange a -1 2
	// 1) "3"
	err = testListRange(key, -1, 2, 3)
	assert.NoError(t, err)

	// 127.0.0.1:6379> lrange a -5 5
	// 1) "1"
	// 2) "2"
	// 3) "3"
	err = testListRange(key, -5, 5, 1, 2, 3)
	assert.NoError(t, err)

	// 127.0.0.1:6379> lrange a -1 0
	// (empty list or set)
	err = testListRange(key, -1, 0)
	assert.NoError(t, err)

	err = testListRange([]byte("empty list"), 0, 100)
	assert.NoError(t, err)

	// 127.0.0.1:6379> lrange a -1 -1
	// 1) "3"
	err = testListRange(key, -1, -1, 3)
	assert.NoError(t, err)

	err = testListIndex(key, -1, 3)
	assert.NoError(t, err)

	err = testListIndex(key, 0, 1)
	assert.NoError(t, err)

	err = testListIndex(key, 1, 2)
	assert.NoError(t, err)

	err = testListIndex(key, 2, 3)
	assert.NoError(t, err)

	err = testListIndex(key, 5, 0)
	assert.NoError(t, err)

	err = testListIndex(key, -1, 3)
	assert.NoError(t, err)

	err = testListIndex(key, -2, 2)
	assert.NoError(t, err)

	err = testListIndex(key, -3, 1)
	assert.NoError(t, err)
}

func TestListMPush(t *testing.T) {
	c := getTestConn()
	defer c.Close()
	key := []byte("TestListMPush-Proxy")
	c.Do("del", key)

	n, err := redis.Int(c.Do("rpush", key, 1, 2, 3))
	assert.NoError(t, err)
	assert.Equal(t, 3, n)

	time.Sleep(50 * time.Millisecond)
	err = testListRange(key, 0, 3, 1, 2, 3)
	assert.NoError(t, err)

	n, err = redis.Int(c.Do("lpush", key, 1, 2, 3))
	assert.NoError(t, err)
	assert.Equal(t, 6, n)

	time.Sleep(50 * time.Millisecond)
	err = testListRange(key, 0, 6, 3, 2, 1, 1, 2, 3)
	assert.NoError(t, err)

}

func TestPop(t *testing.T) {
	c := getTestConn()
	defer c.Close()
	key := []byte("TestPop-Proxy")
	c.Do("del", key)

	n, err := redis.Int(c.Do("rpush", key, 1, 2, 3, 4, 5, 6))
	assert.NoError(t, err)
	assert.Equal(t, 6, n)

	v, err := redis.Int(c.Do("lpop", key))
	assert.NoError(t, err)
	assert.Equal(t, 1, v)

	v, err = redis.Int(c.Do("rpop", key))
	assert.NoError(t, err)
	assert.Equal(t, 6, v)

	n, err = redis.Int(c.Do("lpush", key, 1))
	assert.NoError(t, err)
	assert.Equal(t, 5, n)

	time.Sleep(50 * time.Millisecond)
	err = testListRange(key, 0, 5, 1, 2, 3, 4, 5)
	assert.NoError(t, err)

	for i := 1; i <= 5; i++ {
		v, err := redis.Int(c.Do("lpop", key))
		assert.NoError(t, err)
		assert.Equal(t, i, v)
	}
	time.Sleep(50 * time.Millisecond)
	n, err = redis.Int(c.Do("llen", key))
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

}

func TestListErrorParams(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	_, err := c.Do("lpush", "test_lpush")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("LPUSH").Error(), err.Error())

	_, err = c.Do("rpush", "test_rpush")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("RPUSH").Error(), err.Error())

	_, err = c.Do("lpop", "test_lpop", "a")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("LPOP").Error(), err.Error())

	_, err = c.Do("rpop", "test_rpop", "a")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("RPOP").Error(), err.Error())

	_, err = c.Do("llen", "test_llen", "a")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("LLEN").Error(), err.Error())

	_, err = c.Do("lindex", "test_lindex")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("LINDEX").Error(), err.Error())

	_, err = c.Do("lrange", "test_lrange")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("LRANGE").Error(), err.Error())

	_, err = c.Do("rpoplpush")
	assert.Error(t, err)
	assert.Equal(t, resp.NotFoundErr.Error(), err.Error())
}
