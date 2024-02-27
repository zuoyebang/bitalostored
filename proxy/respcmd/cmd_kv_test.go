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
