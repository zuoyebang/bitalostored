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
	"reflect"
	"testing"
	"time"
)

func checkTxResultIsSame(t *testing.T, cmd interface{}, args ...interface{}) {
	conn := getTestConn()
	defer conn.Close()

	baseConn := getBaseConn()
	defer baseConn.Close()

	var txRes interface{}
	var res interface{}
	var err, txErr error

	command := cmd.(string)
	res, err = baseConn.Do(command, args...)

	_, e := conn.Do("multi")
	if e != nil {
		t.Fatal("multi", command, e)
	}
	_, e = conn.Do(command, args...)
	if e != nil {
		t.Fatal("add command", command, e)
	}
	txRes, txErr = conn.Do("exec")

	if err != nil && txErr != nil {
		if err.Error() != txErr.Error() {
			t.Fatal(err, txErr)
		}
	} else {
		if err != nil || txErr != nil {
			t.Fatal(err, txErr)
		}
	}

	txResultSlice := txRes.([]interface{})
	if len(txResultSlice) <= 0 || reflect.TypeOf(res) != reflect.TypeOf(txResultSlice[0]) {
		t.Fatal(cmd, args, res, txRes)
	}
}

func checkNormalResultIsSame(t *testing.T, cmd interface{}, args ...interface{}) {
	diffConn := getTestConn()
	defer diffConn.Close()

	baseConn := getBaseConn()
	defer baseConn.Close()

	var baseRes, diffRes interface{}
	var baseErr, diffErr error

	command := cmd.(string)
	baseRes, baseErr = baseConn.Do(command, args...)
	diffRes, diffErr = diffConn.Do(command, args...)

	if baseErr != nil && diffErr != nil {
		if baseErr.Error() != diffErr.Error() {
			t.Fatal(cmd, args, baseErr, diffErr)
		}
	} else {
		if baseErr != nil || diffErr != nil {
			t.Fatal(cmd, args, baseErr, diffErr)
		}
	}

	if reflect.TypeOf(baseRes) != reflect.TypeOf(diffRes) {
		t.Fatal(cmd, args, baseRes, diffRes)
	}
}

var kvTestCase [][]interface{} = getKvTestCase()
var hashTestCase [][]interface{} = getHashTestCase()
var listTestCase [][]interface{} = getListTestCase()
var setTestCase [][]interface{} = getSetTestCase()
var zsetTestCase [][]interface{} = getZsetTestCase()

func getKvTestCase() [][]interface{} {
	cmds := make([][]interface{}, 0, 20)
	cmds = append(cmds, []interface{}{"append", "append-kv", "tail"})
	cmds = append(cmds, []interface{}{"set", "setkv", "setkv"})
	cmds = append(cmds, []interface{}{"set", "kv", "kv", "ex", 0})
	cmds = append(cmds, []interface{}{"setex", "setkv", 100, "setkv"})
	cmds = append(cmds, []interface{}{"psetex", "psetexkv", 1000, "setkv"})
	cmds = append(cmds, []interface{}{"setnx", "setnxkv", "setnxkv"})
	cmds = append(cmds, []interface{}{"get", "setkv"})
	cmds = append(cmds, []interface{}{"getset", "setkv", "getsetkv"})

	cmds = append(cmds, []interface{}{"mset", "mset-k1", "mset-v1", "mset-k2", "mset-v2", "mset-k3", "mset-v3", "mset-k4", "mset-v4"})
	cmds = append(cmds, []interface{}{"mget", "mset-k1", "mset-k2", "mset-k3", "mset-k4"})
	cmds = append(cmds, []interface{}{"del", "mset-k1", "mset-k2", "mset-k3", "mset-k4"})

	cmds = append(cmds, []interface{}{"incr", "incrkv"})
	cmds = append(cmds, []interface{}{"incrby", "incrbykv", 10})
	cmds = append(cmds, []interface{}{"incrbyfloat", "incrbyfloatkv", 10.1})
	cmds = append(cmds, []interface{}{"decr", "decrkv"})
	cmds = append(cmds, []interface{}{"decrby", "decrbykv", 10})
	cmds = append(cmds, []interface{}{"exists", "decrbykv"})
	cmds = append(cmds, []interface{}{"exists", "no-exist-kv"})
	cmds = append(cmds, []interface{}{"del", "incrkv"})
	cmds = append(cmds, []interface{}{"del", "del-no-exist-kv"})

	cmds = append(cmds, []interface{}{"expire", "expire-kv", 60})
	cmds = append(cmds, []interface{}{"persist", "persist-kv"})
	cmds = append(cmds, []interface{}{"ttl", "ttl-kv"})
	cmds = append(cmds, []interface{}{"pttl", "pttl-kv"})
	cmds = append(cmds, []interface{}{"type", "type-kv"})
	cmds = append(cmds, []interface{}{"expireat", "expireat-kv", time.Now().Unix() + 60})
	cmds = append(cmds, []interface{}{"pexpire", "pexpire-kv", 10000})
	cmds = append(cmds, []interface{}{"pexpireat", "pexpireat-kv", time.Now().Unix() + 60})

	cmds = append(cmds, []interface{}{"getrange", "getrange-kv", 0, 10})
	cmds = append(cmds, []interface{}{"setrange", "setrange-kv", 4, "abc"})
	cmds = append(cmds, []interface{}{"strlen", "strlen-kv"})
	cmds = append(cmds, []interface{}{"setbit", "setbit-kv", 2, 1})
	cmds = append(cmds, []interface{}{"getbit", "getbit-kv", 2})
	cmds = append(cmds, []interface{}{"bitcount", "getbit-kv"})
	cmds = append(cmds, []interface{}{"bitpos", "getbit-kv", 1, 1, 2})
	return cmds
}

func getHashTestCase() [][]interface{} {
	cmds := make([][]interface{}, 0, 20)
	cmds = append(cmds, []interface{}{"hset", "hset-hash", "name", "hello"})
	cmds = append(cmds, []interface{}{"hget", "hset-hash", "name"})
	cmds = append(cmds, []interface{}{"hget", "hset-hash", "nofield"})
	cmds = append(cmds, []interface{}{"hmset", "hmset-hash", "name", "jim", "age", "10"})
	cmds = append(cmds, []interface{}{"hmget", "hmset-hash", "name", "age"})
	cmds = append(cmds, []interface{}{"hexists", "hmset-hash", "name"})
	cmds = append(cmds, []interface{}{"hdel", "hmset-hash", "name"})
	cmds = append(cmds, []interface{}{"hincrby", "hmset-hash", "age", 10})
	cmds = append(cmds, []interface{}{"hlen", "hlen-hash"})
	cmds = append(cmds, []interface{}{"hgetall", "hgetall-hash"})
	cmds = append(cmds, []interface{}{"hkeys", "hgetall-hash"})
	cmds = append(cmds, []interface{}{"hvals", "hgetall-hash"})
	return cmds
}

func getListTestCase() [][]interface{} {
	cmds := make([][]interface{}, 0, 20)
	cmds = append(cmds, []interface{}{"lpush", "lpush-list", "id-100"})
	cmds = append(cmds, []interface{}{"rpush", "lpush-list", "id-101"})
	cmds = append(cmds, []interface{}{"llen", "lpush-list"})
	cmds = append(cmds, []interface{}{"lindex", "lpush-list", 0})
	cmds = append(cmds, []interface{}{"lrange", "lpush-list", 0, 1})
	cmds = append(cmds, []interface{}{"lset", "lpush-list", 0, "a"})
	cmds = append(cmds, []interface{}{"linsert", "lpush-list", "after", "a", "b"})
	cmds = append(cmds, []interface{}{"lpop", "lpush-list"})
	cmds = append(cmds, []interface{}{"lpush", "lpush-list", "id-100"})
	cmds = append(cmds, []interface{}{"rpop", "lpush-list"})
	cmds = append(cmds, []interface{}{"ltrim", "lpush-list", 0, 0})
	cmds = append(cmds, []interface{}{"lrem", "lpush-list", 1, "a"})

	cmds = append(cmds, []interface{}{"lpushx", "lpush-list-1", "id-10"})
	cmds = append(cmds, []interface{}{"rpushx", "lpush-list-2", "id-10"})
	return cmds
}

func getSetTestCase() [][]interface{} {
	cmds := make([][]interface{}, 0, 20)
	cmds = append(cmds, []interface{}{"sadd", "sadd-set", "id-100"})
	cmds = append(cmds, []interface{}{"sadd", "sadd-set", "id-101"})
	cmds = append(cmds, []interface{}{"sadd", "sadd-set", "id-102"})
	cmds = append(cmds, []interface{}{"scard", "sadd-set"})
	cmds = append(cmds, []interface{}{"sismember", "sadd-set", "id-100"})
	cmds = append(cmds, []interface{}{"smembers", "sadd-set"})
	cmds = append(cmds, []interface{}{"srem", "sadd-set", "id-100"})
	cmds = append(cmds, []interface{}{"spop", "sadd-set"})
	return cmds
}

func getZsetTestCase() [][]interface{} {
	cmds := make([][]interface{}, 0, 20)
	cmds = append(cmds, []interface{}{"zadd", "zadd-set", 100, "id-100"})
	cmds = append(cmds, []interface{}{"zadd", "zadd-set", 101, "id-101"})
	cmds = append(cmds, []interface{}{"zadd", "zadd-set", 102, "id-102"})
	cmds = append(cmds, []interface{}{"zadd", "zadd-set", 102, "id-103"})
	cmds = append(cmds, []interface{}{"zcard", "zadd-set"})
	cmds = append(cmds, []interface{}{"zscore", "zadd-set", "id-100"})
	cmds = append(cmds, []interface{}{"zcount", "zadd-set", 1, 110})
	cmds = append(cmds, []interface{}{"zrange", "zadd-set", 0, -1})
	cmds = append(cmds, []interface{}{"zrevrange", "zadd-set", 0, -1})

	cmds = append(cmds, []interface{}{"zrank", "zadd-set", "id-101"})
	cmds = append(cmds, []interface{}{"zrevrank", "zadd-set", "id-101"})
	cmds = append(cmds, []interface{}{"zincrby", "zadd-set", 3, "id-102"})
	cmds = append(cmds, []interface{}{"zlexcount", "zadd-set", "[id-100", "[id-102"})
	cmds = append(cmds, []interface{}{"zrangebyscore", "zadd-set", 1, 200})
	cmds = append(cmds, []interface{}{"zrevrangebyscore", "zadd-set", 200, 1})
	cmds = append(cmds, []interface{}{"zrangebylex", "zadd-set", "[id-100", "[id-102"})
	cmds = append(cmds, []interface{}{"zrem", "zadd-set", "id-100"})
	cmds = append(cmds, []interface{}{"zremrangebylex", "zadd-set", "[id-101", "[id-101"})
	cmds = append(cmds, []interface{}{"zremrangebyscore", "zadd-set", 102, 102})
	cmds = append(cmds, []interface{}{"zremrangebyrank", "zadd-set", 0, 0})

	cmds = append(cmds, []interface{}{"geoadd", "Sicily", 13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania", 13.583333, 37.316667, "Agrigento"})
	cmds = append(cmds, []interface{}{"geodist", "Sicily", "Palermo", "Catania"})
	cmds = append(cmds, []interface{}{"geohash", "Sicily", "Palermo", "Catania"})
	cmds = append(cmds, []interface{}{"geopos", "Sicily", "Palermo", "Catania"})
	cmds = append(cmds, []interface{}{"georadius", "Sicily", 15, 37, 100, "km"})
	cmds = append(cmds, []interface{}{"georadiusbymember", "Sicily", "Agrigento", 100, "km"})
	return cmds
}

func TestCompareKv(t *testing.T) {
	cmds := kvTestCase
	for _, c := range cmds {
		switch len(c) {
		case 0, 1:
			continue
		default:
			checkNormalResultIsSame(t, c[0], c[1:]...)
		}
	}
}

func TestCompareHash(t *testing.T) {
	cmds := hashTestCase
	for _, c := range cmds {
		switch len(c) {
		case 0, 1:
			continue
		default:
			checkNormalResultIsSame(t, c[0], c[1:]...)
		}
	}
}
func TestCompareList(t *testing.T) {
	cmds := listTestCase
	for _, c := range cmds {
		switch len(c) {
		case 0, 1:
			continue
		default:
			checkNormalResultIsSame(t, c[0], c[1:]...)
		}
	}
}

func TestCompareSet(t *testing.T) {
	cmds := setTestCase
	for _, c := range cmds {
		switch len(c) {
		case 0, 1:
			continue
		default:
			checkNormalResultIsSame(t, c[0], c[1:]...)
		}
	}
}

func TestCompareZset(t *testing.T) {
	cmds := zsetTestCase
	for _, c := range cmds {
		switch len(c) {
		case 0, 1:
			continue
		default:
			checkNormalResultIsSame(t, c[0], c[1:]...)
		}
	}
}
