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

package cmd_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/zuoyebang/bitalostored/stored/internal/resp"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
)

func TestHashCmds(t *testing.T) {
	closeServer, err := startServer(testDBConf, testDBPort)
	require.NoError(t, err)
	defer closeServer()

	time.Sleep(100 * time.Millisecond)

	c := getTestConnWithAddr(testDBPort)
	defer c.Close()

	// Test Hash
	key := []byte("a")
	c.Do("hclear", key)

	if n, err := redis.Int(c.Do("hset", key, []byte("testf1"), []byte("testv1"))); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	res, er := redis.String(c.Do("hget", key, []byte("testf1")))
	require.NoError(t, er)
	require.Equal(t, "testv1", res)

	res, er = redis.String(c.Do("hget", key, []byte("testf1222")))
	require.Equal(t, redis.ErrNil, er)
	require.Equal(t, "", res)

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("hkeyexists", key)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Int(c.Do("hlen", key)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("hset", key, []byte("testf1"), []byte("testv1111"))); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("hlen", key)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("hexists", key, []byte("testf1"))); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("hexists", key, []byte("testf1111"))); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("hdel", key, []byte("testf1"))); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("hlen", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("hdel", key, []byte("testf1"))); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("hlen", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("hset", key, []byte("testf1"), []byte("testv1"))); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("hlen", key)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("hlen", key)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if v, err := redis.Values(c.Do("hgetall", key)); err != nil {
		t.Fatal(err)
	} else if len(v) != 2 {
		t.Fatal(len(v))
	}

	if n, err := redis.Int(c.Do("hclear", key)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("hlen", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("hkeyexists", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	// Test HashM
	keyM := []byte("hash_m")
	c.Do("hclear", keyM)

	if ok, err := redis.String(c.Do("hmset", keyM, "k1", "1", "k2", "2")); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}

	for i := 0; i < readNum; i++ {
		if v, err := redis.Values(c.Do("hmget", keyM, "k1", "k2")); err != nil {
			t.Fatal(err)
		} else {
			if err := testHashArray(v, 1, 2); err != nil {
				t.Fatal(err)
			}
		}
	}

	if n, err := redis.Int(c.Do("hlen", keyM)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if ok, err := redis.String(c.Do("hmset", keyM, "k1", "111", "k2", "222")); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}

	for i := 0; i < readNum; i++ {
		if v, err := redis.Values(c.Do("hmget", keyM, "k1", "k2")); err != nil {
			t.Fatal(err)
		} else {
			if err := testHashArray(v, 111, 222); err != nil {
				t.Fatal(err)
			}
		}
	}

	if n, err := redis.Int(c.Do("hlen", keyM)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("hdel", keyM, "k1", "k2")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("hlen", keyM)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	// Test HashMulitIncr
	keyMultiIncr := []byte("hash_multi_incr")
	c.Do("hclear", keyMultiIncr)

	if v, err := redis.Int64(c.Do("hincrby", keyMultiIncr, "f1", 1)); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal(v)
	}

	if v, err := redis.Int64(c.Do("hincrby", keyMultiIncr, "f1", 10)); err != nil {
		t.Fatal(err)
	} else if v != 11 {
		t.Fatal(v)
	}

	// Test HashIncr
	keyIncr := []byte("hash_incr")
	c.Do("hclear", keyIncr)

	if v, err := redis.Int64(c.Do("hincrby", keyIncr, "f1", 1)); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal(v)
	}

	if v, err := redis.Int64(c.Do("hincrby", keyIncr, "f1", 10)); err != nil {
		t.Fatal(err)
	} else if v != 11 {
		t.Fatal(v)
	}

	// Test HashGetAll
	keyGetAll := []byte("hash_get_all")
	c.Do("hclear", keyGetAll)

	if ok, err := redis.String(c.Do("hmset", keyGetAll, "k1", "1", "k2", "2", "k3", "3")); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}

	if n, err := redis.Int(c.Do("hlen", keyGetAll)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if v, err := redis.Values(c.Do("hgetall", keyGetAll)); err != nil {
		t.Fatal(err)
	} else if len(v) != 6 {
		t.Fatal(len(v))
	}

	if v, err := redis.Values(c.Do("hmget", keyGetAll, "k1", "k2", "k3")); err != nil {
		t.Fatal(err)
	} else {
		if err := testHashArray(v, 1, 2, 3); err != nil {
			t.Fatal(err)
		}
	}

	// Test HashExpireAtAndTTL
	keyExpireAt := []byte("hash_expire_at")
	c.Do("hclear", keyExpireAt)

	if ok, err := redis.String(c.Do("hmset", keyExpireAt, "k1", "1", "k2", "2")); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}

	for i := 0; i < readNum; i++ {
		if v, err := redis.Values(c.Do("hmget", keyExpireAt, "k1", "k2")); err != nil {
			t.Fatal(err)
		} else {
			if err := testHashArray(v, 1, 2); err != nil {
				t.Fatal(err)
			}
		}
	}

	ts := time.Now().Unix()
	expireAt := 10
	if _, err := c.Do("hexpireat", keyExpireAt, ts+int64(expireAt)); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int64(c.Do("httl", keyExpireAt)); err != nil {
			t.Fatal(err)
		} else if n > int64(expireAt) || n < int64(expireAt)-2 {
			t.Fatalf("expect ttl %d, but got %d", expireAt, n)
		}
	}

	time.Sleep(time.Second * time.Duration(expireAt+1))

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("hexists", keyExpireAt, "k1")); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}

	// Test HashErrorParams
	if _, err := c.Do("hset"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hset", "test_hset"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hset", "test_hset", "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hget", "test_hget"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hget", "test_hget", "a", "b"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hexists", "test_hexists"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hexists", "test_hexists", "a", "b"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hdel", "test_hdel"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hlen"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hmset"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hmset", "test_hmset"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hmset", "test_hmset", "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hmget", "test_hmget"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hgetall"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hclear"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hmclear"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hexpire"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hexpireat"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("httl"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hpersist"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	// Test HashExpire
	keyExpire := []byte("kdtemp_activity_drainage_abtest_ED7763B67311E6257A0F05D0E06A6FA3|0")
	if ok, err := redis.String(c.Do("hmset", keyExpire, "kddrainage", 2, "kdstrategytwo", 1, "kddrainagelandingpage", 1, "kddrainagemarket", 1)); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}
	for i := 0; i < readNum; i++ {
		if v, err := redis.Values(c.Do("hmget", keyExpire, "kddrainage", "kdstrategytwo", "kddrainagelandingpage", "kddrainagemarket")); err != nil {
			t.Fatal(err)
		} else {
			if err := testHashArray(v, 2, 1, 1, 1); err != nil {
				t.Fatal(err)
			}
		}
	}

	ts = time.Now().Unix()
	expireAt = 10
	if _, err := c.Do("hexpireat", keyExpire, ts+int64(expireAt)); err != nil {
		t.Fatal(err)
	}
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
