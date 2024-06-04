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

func TestHash(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("TestHash")
	c.Do("del", key)

	n, err := redis.Int(c.Do("hset", key, 1, 0))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	time.Sleep(50 * time.Millisecond)
	n, err = redis.Int(c.Do("hexists", key, 1))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	n, err = redis.Int(c.Do("hexists", key, -1))
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	n, err = redis.Int(c.Do("hget", key, 1))
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	n, err = redis.Int(c.Do("hset", key, 1, 1))
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	time.Sleep(50 * time.Millisecond)
	n, err = redis.Int(c.Do("hget", key, 1))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	n, err = redis.Int(c.Do("hlen", key))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

}

func testHashArray(ay []interface{}, checkValues ...int) error {
	if len(ay) != len(checkValues) {
		return fmt.Errorf("invalid return number %d != %d", len(ay), len(checkValues))
	}

	for i := 0; i < len(ay); i++ {
		if ay[i] == nil && checkValues[i] != 0 {
			return fmt.Errorf("must nil")
		} else if ay[i] != nil {
			v, ok := ay[i].([]byte)
			if !ok {
				return fmt.Errorf("invalid return data %d %v :%T", i, ay[i], ay[i])
			}

			d, _ := strconv.Atoi(string(v))

			if d != checkValues[i] {
				return fmt.Errorf("invalid data %d %s != %d", i, v, checkValues[i])
			}
		}
	}
	return nil
}

func TestHashM(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("TestHashM")
	c.Do("del", key)

	ok, err := redis.String(c.Do("hmset", key, 1, 1, 2, 2, 3, 3))
	assert.NoError(t, err)
	assert.Equal(t, resp.ReplyOK, ok)

	time.Sleep(50 * time.Millisecond)
	n, err := redis.Int(c.Do("hlen", key))
	assert.NoError(t, err)
	assert.Equal(t, 3, n)

	if v, err := redis.Values(c.Do("hmget", key, 1, 2, 3, 4)); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(v, 1, 2, 3, 0); err != nil {
			t.Fatal(err)
		}
	}

	n, err = redis.Int(c.Do("hdel", key, 1, 2, 3, 4))
	assert.NoError(t, err)
	assert.Equal(t, 3, n)

	n, err = redis.Int(c.Do("hlen", key))
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	if v, err := redis.Values(c.Do("hmget", key, 1, 2, 3, 4)); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(v, 0, 0, 0, 0); err != nil {
			t.Fatal(err)
		}
	}

	n, err = redis.Int(c.Do("hlen", key))
	assert.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestHashIncr(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("TestHashIncr")
	c.Do("del", key)

	n, err := redis.Int(c.Do("hincrby", key, 1, 1))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	time.Sleep(50 * time.Millisecond)
	n, err = redis.Int(c.Do("hlen", key))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	n, err = redis.Int(c.Do("hincrby", key, 1, 10))
	assert.NoError(t, err)
	assert.Equal(t, 11, n)

	time.Sleep(50 * time.Millisecond)
	n, err = redis.Int(c.Do("hlen", key))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	n, err = redis.Int(c.Do("hincrby", key, 1, -11))
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

}

func TestHashGetAll(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("TestHashGetAll")
	c.Do("del", key)

	ok, err := redis.String(c.Do("hmset", key, 1, 1, 2, 2, 3, 3))
	assert.NoError(t, err)
	assert.Equal(t, resp.ReplyOK, ok)

	time.Sleep(50 * time.Millisecond)
	if v, err := redis.Values(c.Do("hgetall", key)); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(v, 1, 1, 2, 2, 3, 3); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("hkeys", key)); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(v, 1, 2, 3); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("hvals", key)); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(v, 1, 2, 3); err != nil {
			t.Fatal(err)
		}
	}

	n, err := redis.Int(c.Do("hlen", key))
	assert.NoError(t, err)
	assert.Equal(t, 3, n)
}

func TestHashErrorParams(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	_, err := c.Do("hset", "test_hset")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("HSET").Error(), err.Error())

	_, err = c.Do("hget", "test_hget")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("HGET").Error(), err.Error())

	_, err = c.Do("hexists", "test_hexists")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("HEXISTS").Error(), err.Error())

	_, err = c.Do("hdel", "test_hdel")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("HDEL").Error(), err.Error())

	_, err = c.Do("hlen", "test_hlen", "a")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("HLEN").Error(), err.Error())

	_, err = c.Do("hincrby", "test_hincrby")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("HINCRBY").Error(), err.Error())

	_, err = c.Do("hmset", "test_hmset")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("HMSET").Error(), err.Error())

	_, err = c.Do("hmset", "test_hmset", "f1", "v1", "f2")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("HMSET").Error(), err.Error())

	_, err = c.Do("hmget", "test_hget")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("HMGET").Error(), err.Error())

	_, err = c.Do("hgetall")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("HGETALL").Error(), err.Error())

	_, err = c.Do("hkeys")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("HKEYS").Error(), err.Error())

	_, err = c.Do("hvals")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("HVALS").Error(), err.Error())

	_, err = c.Do("hscan")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("HSCAN").Error(), err.Error())

}
