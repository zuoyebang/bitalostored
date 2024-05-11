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
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
)

func TestHash(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("a")
	c.Do("hclear", key)

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("hkeyexists", key)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Int(c.Do("hset", key, 1, 0)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("hkeyexists", key)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("hexists", key, 1)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("hexists", key, -1)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("hget", key, 1)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Int(c.Do("hset", key, 1, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("hget", key, 1)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("hlen", key)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
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

func TestHashM(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("b")
	c.Do("hclear", key)
	if ok, err := redis.String(c.Do("hmset", key, 1, 1, 2, 2, 3, 3)); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("hlen", key)); err != nil {
			t.Fatal(err)
		} else if n != 3 {
			t.Fatal(n)
		}

		if v, err := redis.Values(c.Do("hmget", key, 1, 2, 3, 4)); err != nil {
			t.Fatal(err)
		} else {
			if err := testHashArray(v, 1, 2, 3, 0); err != nil {
				t.Fatal(err)
			}
		}
	}

	if n, err := redis.Int(c.Do("hdel", key, 1, 2, 3, 4)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("hlen", key)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if v, err := redis.Values(c.Do("hmget", key, 1, 2, 3, 4)); err != nil {
			t.Fatal(err)
		} else {
			if err := testHashArray(v, 0, 0, 0, 0); err != nil {
				t.Fatal(err)
			}
		}

		if n, err := redis.Int(c.Do("hlen", key)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}
}

func TestHashMulitIncr(t *testing.T) {
	key := []byte("HashMulitIncr")
	field := "a"

	c := getTestConn()
	defer c.Close()
	c.Do("del", key)

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			c := getTestConn()
			defer c.Close()
			defer wg.Done()

			for j := 0; j < 500; j++ {
				if _, err := redis.Int(c.Do("hincrby", key, field, 1)); err != nil {
					t.Fatal(err)
				}
			}
		}()
	}
	wg.Wait()
	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("hget", key, field)); err != nil {
			t.Fatal(err)
		} else if n != 5000 {
			t.Fatal(n)
		}
	}
}

func TestHashIncr(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("c")
	c.Do("hclear", key)

	if n, err := redis.Int(c.Do("hincrby", key, 1, 1)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(err)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("hlen", key)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Int(c.Do("hincrby", key, 1, 10)); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(err)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("hlen", key)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Int(c.Do("hincrby", key, 1, -11)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(err)
	}
}

func TestHashMulitIncrby(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key1 := []byte("kd_codesearch_lotteryprizerest_hash_16")
	key2 := []byte("kd_codesearch_lotteryprizerest_hash_1666")
	c.Do("del", key1, key2)

	var key14n, key16n, key24n, key26n int
	for j := 0; j < 500; j++ {
		key14n += -1
		if n, err := redis.Int(c.Do("hincrby", key1, 4, -1)); err != nil {
			t.Fatal(err)
		} else if n != key14n {
			t.Fatal(err)
		}

		key16n += -1
		if n, err := redis.Int(c.Do("hincrby", key1, 6, -1)); err != nil {
			t.Fatal(err)
		} else if n != key16n {
			t.Fatal(err)
		}

		key24n += -1
		if n, err := redis.Int(c.Do("hincrby", key2, 4, -1)); err != nil {
			t.Fatal(err)
		} else if n != key24n {
			t.Fatal(err)
		}

		key26n += -1
		if n, err := redis.Int(c.Do("hincrby", key2, 6, -1)); err != nil {
			t.Fatal(err)
		} else if n != key26n {
			t.Fatal(err)
		}

		if j%10 == 0 {
			for i := 0; i < readNum; i++ {
				if v, err := redis.Values(c.Do("hgetall", key1)); err != nil {
					t.Fatal(err)
				} else {
					if err := testHashArray(v, 4, key14n, 6, key16n); err != nil {
						t.Fatal(err)
					}
				}
				if v, err := redis.Values(c.Do("hgetall", key2)); err != nil {
					t.Fatal(err)
				} else {
					if err := testHashArray(v, 4, key24n, 6, key26n); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}
}

func TestHashGetAll(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("d")
	c.Do("hclear", key)

	if ok, err := redis.String(c.Do("hmset", key, 1, 1, 2, 2, 3, 3)); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}

	for i := 0; i < readNum; i++ {
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
	}

	if n, err := redis.Int(c.Do("hclear", key)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("hlen", key)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}
}

func TestHashExpireAtAndTTL(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("test-hash-expire")
	field := []byte("a")
	value := []byte("1")

	var err error

	if _, err := c.Do("hset", key, field, value); err != nil {
		t.Fatal(err)
	}

	ts := time.Now().Unix()
	expireAt := 3
	if _, err := c.Do("hexpireAt", key, ts+int64(expireAt)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < readNum; i++ {
		if ttl, err := c.Do("httl", key); err != nil {
			t.Fatal(err)
		} else {
			ttlValue, ok := ttl.(int64)
			if !ok {
				t.Fatal(ok)
			}
			if ttlValue < 0 || ttlValue > int64(expireAt+1) {
				t.Fatalf("hash ttl error, (0-%d) vs %d", expireAt, ttlValue)
			}
		}
	}
	time.Sleep(time.Duration(expireAt+1) * time.Second)

	for i := 0; i < readNum; i++ {
		if v, err := c.Do("hkeyexists", key); err != nil {
			t.Fatal(err)
		} else {
			exist, ok := v.(int64)
			if !ok {
				t.Fatal(ok)
			}
			if exist != 0 {
				t.Fatal("hash exist error")
			}
		}
	}

	if _, err = c.Do("hclear", key); err != nil {
		t.Fatal(err)
	}
}

func TestHashErrorParams(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	if _, err := c.Do("hset", "test_hset"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hget", "test_hget"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hexists", "test_hexists"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hdel", "test_hdel"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hlen", "test_hlen", "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hincrby", "test_hincrby"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hmset", "test_hmset"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hmset", "test_hmset", "f1", "v1", "f2"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hmget", "test_hget"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hgetall"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hkeys"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hvals"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hclear"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hclear", "test_hclear", "a"); err != nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hexpire", "test_hexpire"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hexpireat", "test_hexpireat"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("httl"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("hpersist"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}
}

func TestHashExpire(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("kdtemp_activity_drainage_abtest_ED7763B67311E6257A0F05D0E06A6FA3|0")
	if ok, err := redis.String(c.Do("hmset", key, "kddrainage", 2, "kdstrategytwo", 1, "kddrainagelandingpage", 1, "kddrainagemarket", 1)); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}
	for i := 0; i < readNum; i++ {
		if v, err := redis.Values(c.Do("hmget", key, "kddrainage", "kdstrategytwo", "kddrainagelandingpage", "kddrainagemarket")); err != nil {
			t.Fatal(err)
		} else {
			if err := testHashArray(v, 2, 1, 1, 1); err != nil {
				t.Fatal(err)
			}
		}
	}

	ts := time.Now().Unix()
	expireAt := 10
	if _, err := c.Do("expireat", key, ts+int64(expireAt)); err != nil {
		t.Fatal(err)
	}
}

func TestHashConcurrencySet(t *testing.T) {
	var wg sync.WaitGroup
	var kid atomic.Uint64

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			c := getTestConn()
			defer func() {
				c.Close()
				wg.Done()
			}()
			for j := 0; j < 1000; j++ {
				key := fmt.Sprintf("TestHashConcurrencySet_%d", kid.Add(1))
				c.Do("del", key)
				if n, err := redis.Int(c.Do("hset", key, "hash_field", key)); err != nil {
					t.Fatal(err)
				} else if n != 1 {
					t.Fatal(n)
				}
			}
		}()
	}
	wg.Wait()

	require.Equal(t, uint64(100000), kid.Load())

	c := getTestConn()
	defer c.Close()
	for i := 1; i <= 100000; i++ {
		for j := 0; j < readNum; j++ {
			key := fmt.Sprintf("TestHashConcurrencySet_%d", i)
			if v, err := redis.String(c.Do("hget", key, "hash_field")); err != nil {
				t.Fatal(err)
			} else if v != key {
				t.Fatalf("get fail exp:%s act:%s", key, v)
			}
		}
	}
}
