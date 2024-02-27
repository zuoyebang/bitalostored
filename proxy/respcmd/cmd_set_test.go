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

	"github.com/zuoyebang/bitalostored/proxy/resp"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	c := getTestConn()
	defer c.Close()
	key1 := "testdb_cmd_set_1"
	key2 := "testdb_cmd_set_2"
	c.Do("del", key1, key2)

	n, err := redis.Int(c.Do("sadd", key1, 0, 1))
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	n, err = redis.Int(c.Do("sadd", key2, 0, 1, 2, 3))
	assert.NoError(t, err)
	assert.Equal(t, 4, n)

	n, err = redis.Int(c.Do("scard", key1))
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	n, err = redis.Int(c.Do("srem", key1, 0, 1))
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	if n, err := redis.Int(c.Do("sismember", key2, 0)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	vals, err := redis.Values(c.Do("smembers", key2))
	assert.NoError(t, err)
	assert.Equal(t, 4, len(vals))
}

func TestSetErrorParams(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	_, err := c.Do("sadd", "test_sadd")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("SADD").Error(), err.Error())

	_, err = c.Do("scard")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("SCARD").Error(), err.Error())

	_, err = c.Do("scard", "k1", "k2")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("SCARD").Error(), err.Error())

	_, err = c.Do("sismember", "k1")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("SISMEMBER").Error(), err.Error())

	_, err = c.Do("sismember", "k1", "m1", "m2")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("SISMEMBER").Error(), err.Error())

	_, err = c.Do("smembers")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("SMEMBERS").Error(), err.Error())

	_, err = c.Do("smembers", "k1", "k2")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("SMEMBERS").Error(), err.Error())

	_, err = c.Do("srem")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("SREM").Error(), err.Error())

	_, err = c.Do("srem", "key")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("SREM").Error(), err.Error())

	_, err = c.Do("sdiff")
	assert.Error(t, err)
	assert.Equal(t, resp.NotFoundErr.Error(), err.Error())
}
