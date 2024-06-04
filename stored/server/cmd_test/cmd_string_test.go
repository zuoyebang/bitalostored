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
	"crypto/md5"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
)

const defaultValBytes = "1qaz2wsx3edc4rfv5tgb6yhn7ujm8ik9ol0p1qaz2wsx3edc4rfv5tgb6yhn7ujm8ik9ol0p1qaz2wsx3edc4rfv5tgb6yhn7ujm8ik9ol0p"

func TestKVSet(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := fmt.Sprintf("%x", md5.Sum([]byte("xingfu")))
	val := "helloworldhelloworld"
	if ok, err := redis.String(c.Do("set", key, val)); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}
}

func TestKVSetEx(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := "test_setex"
	val1 := "hello world1"
	if ok, err := redis.String(c.Do("setex", key, 1000, val1)); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}
	for i := 0; i < readNum; i++ {
		if n, err := redis.Int64(c.Do("ttl", key)); err != nil {
			t.Fatal(err)
		} else if n != 1000 {
			t.Fatalf("ttl fail exp:%d act:%d", 1000, n)
		}
		if v, err := redis.String(c.Do("get", key)); err != nil {
			t.Fatal(err)
		} else if v != val1 {
			t.Fatalf("get fail exp:%s act:%s", val1, v)
		}
	}

	val2 := "hello world2"
	if ok, err := redis.String(c.Do("setex", key, 100, val2)); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}
	for i := 0; i < readNum; i++ {
		if n, err := redis.Int64(c.Do("ttl", key)); err != nil {
			t.Fatal(err)
		} else if n != 100 {
			t.Fatalf("ttl fail exp:%d act:%d", 100, n)
		}
		if v, err := redis.String(c.Do("get", key)); err != nil {
			t.Fatal(err)
		} else if v != val2 {
			t.Fatalf("get fail exp:%s act:%s", val2, v)
		}
	}

	val3 := "hello world3"
	if ok, err := redis.String(c.Do("psetex", key, 1300, val3)); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}
	for i := 0; i < readNum; i++ {
		if n, err := redis.Int64(c.Do("pttl", key)); err != nil {
			t.Fatal(err)
		} else if n != 1300 {
			t.Fatalf("ttl fail exp:%d act:%d", 1300, n)
		}
		if n, err := redis.Int64(c.Do("ttl", key)); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatalf("ttl fail exp:%d act:%d", 2, n)
		}
		if v, err := redis.String(c.Do("get", key)); err != nil {
			t.Fatal(err)
		} else if v != val3 {
			t.Fatalf("get fail exp:%s act:%s", val3, v)
		}
	}

	time.Sleep(time.Second * 2)
	for i := 0; i < readNum; i++ {
		if _, err := redis.String(c.Do("get", key)); err != redis.ErrNil {
			t.Fatal(err)
		}
		if n, err := redis.Int(c.Do("exists", key)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}

	if ok, err := redis.String(c.Do("set", key, val3, "px", 1500, "nx")); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}
	for i := 0; i < readNum; i++ {
		if v, err := redis.String(c.Do("get", key)); err != nil {
			t.Fatal(err)
		} else if v != val3 {
			t.Fatalf("get fail exp:%s act:%s", val3, v)
		}
	}

	time.Sleep(time.Second * 2)
	for i := 0; i < readNum; i++ {
		if _, err := redis.String(c.Do("get", key)); err != redis.ErrNil {
			t.Fatal(err)
		}
		if n, err := redis.Int(c.Do("exists", key)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}
}

func TestKVSet1(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	for i := 0; i <= 50; i++ {
		newKey := []byte(fmt.Sprintf("key_%d", i))
		newValue := []byte(fmt.Sprintf("%s_%s", newKey, defaultValBytes))
		if ok, err := redis.String(c.Do("set", newKey, newValue)); err != nil {
			t.Fatal(err)
		} else if ok != resp.ReplyOK {
			t.Fatal(ok)
		}
	}
}

func TestKVGet1(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	for i := 0; i <= 50; i++ {
		newKey := []byte(fmt.Sprintf("key_%d", i))
		newValue := []byte(fmt.Sprintf("%s_%s", newKey, defaultValBytes))
		for i := 0; i < readNum; i++ {
			v, err := redis.String(c.Do("get", newKey))
			require.NoError(t, err)
			require.Equal(t, string(newValue), v)
		}
	}
}

func TestKVMsetAndDel(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	if ok, err := redis.String(c.Do("mset", "a", "1", "a", "2")); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}

	if n, err := redis.Int(c.Do("del", "a", "a")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
}

func TestKV(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	if ok, err := redis.String(c.Do("set", "aabbvv", "1234")); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}

	if n, err := redis.Int(c.Do("setnx", "aabbvv", "123")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	c.Do("del", "bbcccvv")
	if n, err := redis.Int(c.Do("setnx", "bbcccvv", "123")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if ok, err := redis.String(c.Do("setex", "xx", 10, "hello world")); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}

	c.Do("del", "kvexnx")
	if ok, err := redis.String(c.Do("set", "kvexnx", "hello", "ex", 10, "nx")); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}

	if ok, err := redis.String(c.Do("set", "kvexnx", "hello", "ex", 10, "nx")); err != nil && err != redis.ErrNil {
		t.Fatal(err)
	} else if ok == resp.ReplyOK {
		t.Fatal(ok)
	}

	for i := 0; i < readNum; i++ {
		if v, err := redis.String(c.Do("get", "aabbvv")); err != nil {
			t.Fatal(err)
		} else if v != "1234" {
			t.Fatal(v)
		}
	}

	if v, err := redis.String(c.Do("getset", "aabbvv", "123")); err != nil {
		t.Fatal(err)
	} else if v != "1234" {
		t.Fatal(v)
	}

	for i := 0; i < readNum; i++ {
		if v, err := redis.String(c.Do("get", "aabbvv")); err != nil {
			t.Fatal(err)
		} else if v != "123" {
			t.Fatal(v)
		}

		if n, err := redis.Int(c.Do("exists", "aabbvv")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("exists", "empty_key_test")); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}

	if _, err := redis.Int(c.Do("del", "aabbvv", "bbcccvv")); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("exists", "aabbvv")); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("exists", "bbcccvv")); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}

	rangeKey := "range_key"
	c.Do("del", rangeKey)
	if n, err := redis.Int(c.Do("append", rangeKey, "Hello ")); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("setrange", rangeKey, 6, "Redis")); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("strlen", rangeKey)); err != nil {
			t.Fatal(err)
		} else if n != 11 {
			t.Fatal(n)
		}
	}

	for i := 0; i < readNum; i++ {
		if v, err := redis.String(c.Do("getrange", rangeKey, 0, -1)); err != nil {
			t.Fatal(err)
		} else if v != "Hello Redis" {
			t.Fatal(v)
		}

		if v, err := redis.String(c.Do("getrange", rangeKey, 0, 5)); err != nil {
			t.Fatal(err)
		} else if v != "Hello " {
			t.Fatal(v)
		}
	}

	bitKey := "bit_key"
	c.Do("del", bitKey)
	if n, err := redis.Int(c.Do("setbit", bitKey, 7, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("getbit", bitKey, 7)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("bitcount", bitKey)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("bitpos", bitKey, 1)); err != nil {
			t.Fatal(err)
		} else if n != 7 {
			t.Fatal(n)
		}
	}
}

func TestKVM(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	if ok, err := redis.String(c.Do("mset", "a", "1", "b", "2")); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}

	for i := 0; i < readNum; i++ {
		if v, err := redis.Values(c.Do("mget", "a", "b", "c")); err != nil {
			t.Fatal(err)
		} else if len(v) != 3 {
			t.Fatal(len(v))
		} else {
			if vv, ok := v[0].([]byte); !ok || string(vv) != "1" {
				t.Fatal("not 1")
			}

			if vv, ok := v[1].([]byte); !ok || string(vv) != "2" {
				t.Fatal("not 2")
			}

			if v[2] != nil {
				t.Fatal("must nil")
			}
		}
	}
}

func TestKVIncrDecr(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := "n"
	c.Do("del", key)
	if n, err := redis.Int64(c.Do("incr", key)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("incr", key)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("decr", key)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("incrby", key, 10)); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("decrby", key, 10)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
}

func TestKVMuchIncr(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := "n"
	c.Do("del", key)
	for i := 0; i < 100; i++ {
		if n, err := redis.Int64(c.Do("incrby", key, 1)); err != nil {
			t.Fatal(err)
		} else if n != int64(i+1) {
			t.Fatal(n)
		}
	}
}

func TestKVIncrFloat(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := "n"
	c.Do("del", key)
	if n, err := redis.String(c.Do("incrbyfloat", key)); err == nil {
		t.Fatal(n)
	} else if err.Error() != "ERR wrong number of arguments for 'incrbyfloat' command" {
		t.Fatal(err.Error())
	}
	if n, err := redis.String(c.Do("incrbyfloat", key, 10.50)); err != nil {
		t.Fatal(err)
	} else if n != "10.5" {
		t.Fatal(n)
	}

	if n, err := redis.String(c.Do("incrbyfloat", key, 0.1)); err != nil {
		t.Fatal(err)
	} else if n != "10.6" {
		t.Fatal(n)
	}

	if n, err := redis.String(c.Do("incrbyfloat", key, -5)); err != nil {
		t.Fatal(err)
	} else if n != "5.6" {
		t.Fatal(n)
	}
	if n, err := redis.String(c.Do("incrbyfloat", key, 0.000000001)); err != nil {
		t.Fatal(err)
	} else if n != "5.600000001" {
		t.Fatal(n)
	}

	c.Do("set", key, 5.0e3)

	if n, err := redis.String(c.Do("incrbyfloat", key, 2.0e2)); err != nil {
		t.Fatal(err)
	} else if n != "5200" {
		t.Fatal(n)
	}
}

func TestKVErrorParams(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	if _, err := c.Do("get", "a", "b", "c"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := c.Do("set", "a", "b", "c"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := c.Do("getset", "a", "b", "c"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := c.Do("setnx", "a", "b", "c"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := c.Do("exists", "a", "b"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := c.Do("incr", "a", "b"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := c.Do("incrby", "a"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := c.Do("decrby", "a"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := c.Do("del"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("mset"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("mset", "a", "b", "c"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("mget"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("expire"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("expire", "a", "b"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("expireat"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("expireat", "a", "b"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("ttl"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("persist"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("setex", "a", "blah", "hello world"); err == nil {
		t.Fatalf("invalid err %v", err)
	}
}

func TestKVConcurrencySet(t *testing.T) {
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
				key := fmt.Sprintf("TestKVConcurrencySet_%d", kid.Add(1))
				if ok, err := redis.String(c.Do("set", key, key)); err != nil {
					t.Fatal(err)
				} else if ok != resp.ReplyOK {
					t.Fatal(ok)
				}
			}
		}()
	}
	wg.Wait()

	require.Equal(t, uint64(100000), kid.Load())

	c := getTestConn()
	defer c.Close()
	for i := 1; i <= 100000; i++ {
		key := fmt.Sprintf("TestKVConcurrencySet_%d", i)
		for i := 0; i < readNum; i++ {
			if v, err := redis.String(c.Do("get", key)); err != nil {
				t.Fatal(err)
			} else if v != key {
				t.Fatalf("get fail exp:%s act:%s", key, v)
			}
		}
	}
}
