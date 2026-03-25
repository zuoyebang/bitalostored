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
	"github.com/zuoyebang/bitalostored/proxy/internal/errn"
	"github.com/zuoyebang/bitalostored/proxy/internal/utils"
	"strings"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func TestKV(t *testing.T) {
	c := getTestConn()
	defer c.Close()
	c.Do("del", "a", "b", "xx", "empty_key_test")

	ok, err := redis.String(c.Do("set", "a", "1234"))
	assert.NoError(t, err)
	assert.Equal(t, "OK", ok)

	ok, err = redis.String(c.Do("setex", "xx", 10, "hello world"))
	assert.NoError(t, err)
	assert.Equal(t, "OK", ok)

	ttl, err := redis.Int64(c.Do("ttl", "a"))
	assert.Equal(t, int64(-1), ttl)
	assert.NoError(t, err)

	ttl, err = redis.Int64(c.Do("ttl", "xx"))
	assert.NotEqual(t, int64(-1), ttl)
	assert.NoError(t, err)

	v, err := redis.String(c.Do("get", "a"))
	assert.NoError(t, err)
	assert.Equal(t, "1234", v)

	res, err := redis.Bool(c.Do("persist", "xx"))
	assert.NoError(t, err)
	assert.True(t, res)

	time.Sleep(50 * time.Millisecond)
	ttl, err = redis.Int64(c.Do("ttl", "xx"))
	assert.Equal(t, int64(-1), ttl)
	assert.NoError(t, err)

	when := time.Now().Unix() + 100
	expireAtKey := "expireAtTestKey"
	c.Do("set", expireAtKey, "test-value")
	_, err = redis.Int(c.Do("expireat", expireAtKey, when))
	assert.NoError(t, err)
	ts, err := redis.Int(c.Do("ttl", expireAtKey))
	assert.NoError(t, err)
	if ts <= 0 || ts > 100 {
		t.Errorf("ttl error. ts:%d", ts)
	}
	c.Do("del", expireAtKey)

	pexpireAtKey := "pexpireAtTestKey"
	c.Do("set", pexpireAtKey, "test-value")
	when = time.Now().Unix() + 100
	_, err = redis.Int(c.Do("pexpireat", pexpireAtKey, when*1000))
	assert.NoError(t, err)
	ts, err = redis.Int(c.Do("ttl", pexpireAtKey))
	assert.NoError(t, err)
	if ts <= 0 || ts > 100 {
		t.Errorf("ttl error. ts:%d", ts)
	}
	c.Do("del", pexpireAtKey)

	time.Sleep(50 * time.Millisecond)
	ttl, err = redis.Int64(c.Do("ttl", "xx"))
	assert.Equal(t, int64(-1), ttl)
	assert.NoError(t, err)

	n, err := redis.Int(c.Do("setnx", "a", "5678"))
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	n, err = redis.Int(c.Do("exists", "a"))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	n, err = redis.Int(c.Do("exists", "empty_key_test"))
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	n, err = redis.Int(c.Do("del", "a", "b"))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	time.Sleep(50 * time.Millisecond)
	n, err = redis.Int(c.Do("exists", "a"))
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	n, err = redis.Int(c.Do("exists", "b"))
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

}

func TestKVM(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	ok, err := redis.String(c.Do("mset", "amset", "1", "bmset", "2"))
	assert.NoError(t, err)
	assert.Equal(t, "OK", ok)

	v, err := redis.ByteSlices(c.Do("mget", "amset", "bmset", "cmget"))
	assert.NoError(t, err)
	assert.Equal(t, 3, len(v))
	assert.Equal(t, "1", string(v[0]))
	assert.Equal(t, "2", string(v[1]))
	assert.Nil(t, v[2])

}

func TestKVIncrDecr(t *testing.T) {
	c := getTestConn()
	defer c.Close()
	c.Do("del", "TestKVIncrDecr")

	n, err := redis.Int(c.Do("incr", "TestKVIncrDecr"))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	n, err = redis.Int(c.Do("incr", "TestKVIncrDecr"))
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	n, err = redis.Int(c.Do("decr", "TestKVIncrDecr"))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	n, err = redis.Int(c.Do("incrby", "TestKVIncrDecr", 10))
	assert.NoError(t, err)
	assert.Equal(t, 11, n)

	n, err = redis.Int(c.Do("decrby", "TestKVIncrDecr", 10))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestStringKeyError(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := strings.Repeat("S", invalieKeySize)
	v := "value"

	commands := [][]string{
		{"set", key, v},
		{"get", key},
		{"append", key, v},
		{"getset", key, v},
		{"setex", key, "10", v},
		{"setnx", key, v},
		{"exists", key},
		{"incr", key},
		{"decr", key},
		{"incrby", key, "1"},
		{"decrby", key, "1"},
		{"incrbyfloat", key, "1"},
		{"expire", key, "10"},
		{"pexpire", key, "10"},
		{"pexpireat", key, "10"},
		{"expireat", key, "10"},
		{"ttl", key},
		{"pttl", key},
		{"persist", key},
		{"setrange", key, "0", "aa"},
		{"strlen", key},
		{"getbit", key, "100"},
		{"setbit", key, "100", "1"},
		{"bitcount", key, "0", "100"},
		{"bitpos", key, "100"},
	}

	for _, args := range commands {
		if len(args) <= 1 {
			continue
		}
		inputs := make([]interface{}, 0, len(args)-1)
		for _, arg := range args[1:] {
			inputs = append(inputs, arg)
		}
		_, err := c.Do(args[0], inputs...)
		assert.Error(t, err)
		assert.Equal(t, errn.ErrKeySize.Error(), err.Error())
	}
}
