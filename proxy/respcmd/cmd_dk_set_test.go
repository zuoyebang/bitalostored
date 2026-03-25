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

var DkSetKey = []byte("TestDkSet")

func TestDkSet(t *testing.T) {
	c := getTestConn()
	defer c.Close()
	_, err := c.Do("dk.del", "77c44df457908f56", DkSetKey)
	if err != nil {
		t.Fatalf("dk.del key:%s err:%v", DkSetKey, err)
	}

	_, err = c.Do("dk.screate", DkSetKey, 2, "77c44df457908f56")
	if !assert.NoError(t, err) {
		return
	}
	n, err := redis.Int(c.Do("dk.sadd", DkSetKey, "a", "b", "c", "e", "f", "g"))
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equal(t, 6, n) {
		return
	}
	n, err = redis.Int(c.Do("dk.sadd", DkSetKey, "a", "b", "c", "e"))
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equal(t, 0, n) {
		return
	}
	n, err = redis.Int(c.Do("dk.scard", DkSetKey))
	assert.NoError(t, err)
	assert.Equal(t, 6, n)
	n, err = redis.Int(c.Do("dk.srem", DkSetKey, "a", "b"))
	assert.NoError(t, err)
	assert.Equal(t, 2, n)
	n, err = redis.Int(c.Do("dk.sismember", DkSetKey, "c"))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	r, err := redis.ByteSlices(c.Do("dk.spop", DkSetKey))
	assert.NoError(t, err)
	if len(r) != 1 {
		t.Fatal("dk.spop empty")
	}

	r, err = redis.ByteSlices(c.Do("dk.spop", DkSetKey, 2))
	assert.NoError(t, err)
	if len(r) != 2 {
		t.Fatal("dk.spop number failed")
	}

	n, err = redis.Int(c.Do("dk.scard", DkSetKey))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	r, err = redis.ByteSlices(c.Do("dk.spop", DkSetKey, 2))
	assert.NoError(t, err)
	if len(r) != 1 {
		t.Fatal("dk.spop number failed")
	}
}

func TestDKSetError(t *testing.T) {
	c := getTestConn()
	defer c.Close()
	unexistsKey := []byte("hahaha")

	_, err := c.Do("dk.srem", unexistsKey, "a", "b")
	assert.Error(t, err)
	assert.Equal(t, resp.ErrDKkeyNotFound.Error(), err.Error())

	_, err = c.Do("dk.spop", unexistsKey)
	assert.Error(t, err)
	assert.Equal(t, resp.ErrDKkeyNotFound.Error(), err.Error())

	_, err = c.Do("dk.sadd", unexistsKey, "a")
	assert.Error(t, err)
	assert.Equal(t, resp.ErrDKkeyNotFound.Error(), err.Error())
}
