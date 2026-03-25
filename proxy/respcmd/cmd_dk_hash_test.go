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
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/zuoyebang/bitalostored/proxy/resp"
)

var DkHashKey = []byte("TestDkHash")

func TestDkHash(t *testing.T) {
	c := getTestConn()
	defer c.Close()
	_, err := c.Do("dk.del", "77c44df457908f56", DkHashKey)
	if err != nil {
		t.Fatalf("dk.del key:%s err:%v", DkHashKey, err)
	}

	_, err = c.Do("dk.hcreate", DkHashKey, 10, "77c44df457908f56")
	if !assert.NoError(t, err) {
		return
	}
	n, err := redis.Int(c.Do("dk.hset", DkHashKey, "a", 1, "b", 1, "c", 1, "d", 1))
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, 4, n)

	n, err = redis.Int(c.Do("dk.hlen", DkHashKey))
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, 4, n)

	//old field
	n, err = redis.Int(c.Do("dk.HINCRBY", DkHashKey, "a", 1))
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, 2, n)

	n, err = redis.Int(c.Do("dk.hexists", DkHashKey, "a"))
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, 1, n)

	n, err = redis.Int(c.Do("dk.hexists", DkHashKey, "hahah"))
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, 0, n)

	n, err = redis.Int(c.Do("dk.hget", DkHashKey, "a"))
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, 2, n)

	if v, err := redis.Values(c.Do("dk.hmget", DkHashKey, "a", "b", "c", "d")); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(v, 2, 1, 1, 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("dk.hmget", DkHashKey, "a")); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(v, 2); err != nil {
			t.Fatal(err)
		}
	}

	n, err = redis.Int(c.Do("dk.hdel", DkHashKey, "b", "c"))
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, 2, n)

	//new field
	n, err = redis.Int(c.Do("dk.hincrby", DkHashKey, "e", 1))
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, 1, n)
}

func TestDkHashErr(t *testing.T) {
	c := getTestConn()
	defer c.Close()
	key := []byte("hashnotexists")
	_, err := c.Do("dk.hcreate", key, 2000)
	assert.Error(t, err)
	assert.Equal(t, resp.ValueErr.Error(), err.Error())

	_, err = redis.Int(c.Do("dk.hset", key, 1, 1))
	assert.Error(t, err)
	assert.Equal(t, resp.ErrDKkeyNotFound.Error(), err.Error())
}
