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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zuoyebang/bitalostored/stored/internal/resp"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
)

func TestStringCmds(t *testing.T) {
	closeServer, err := startServer(testDBConf, testDBPort)
	require.NoError(t, err)
	defer closeServer()

	time.Sleep(100 * time.Millisecond)

	c := getTestConnWithAddr(testDBPort)
	defer c.Close()

	// Test basic SET/GET operations
	key1 := []byte("testkvkey1_unique")
	val1 := testRandBytes(6 << 20)
	ok, err := redis.String(c.Do("set", key1, val1))
	require.NoError(t, err)
	require.Equal(t, resp.ReplyOK, ok)
	v, err := redis.String(c.Do("get", key1))
	require.NoError(t, err)
	require.Equal(t, string(val1), v)

	key2 := []byte("testkvkey2_unique")
	val2 := testRandBytes(6 << 20)
	ok, err = redis.String(c.Do("set", key2, val2))
	require.NoError(t, err)
	require.Equal(t, resp.ReplyOK, ok)
	v, err = redis.String(c.Do("get", key2))
	require.NoError(t, err)
	require.Equal(t, string(val2), v)

	// Test SETNX operations
	setnxKey := "test_setnx_unique"
	val1SetNx := "hello world1"

	if n, err := redis.Int(c.Do("setnx", setnxKey, val1SetNx)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if v, err := redis.String(c.Do("get", setnxKey)); err != nil {
		t.Fatal(err)
	} else if v != val1SetNx {
		t.Fatalf("get fail exp:%s act:%s", val1SetNx, v)
	}

	newValSetNx := "new value"
	if n, err := redis.Int(c.Do("setnx", setnxKey, newValSetNx)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if v, err := redis.String(c.Do("get", setnxKey)); err != nil {
		t.Fatal(err)
	} else if v != val1SetNx {
		t.Fatalf("get fail exp:%s act:%s", val1SetNx, v)
	}

	// Test MGET operations
	if ok, err := redis.String(c.Do("mset", "a_unique", "1", "b_unique", "2", "c_unique", "3")); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}

	if v, err := redis.Values(c.Do("mget", "a_unique", "b_unique", "c_unique")); err != nil {
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

		if vv, ok := v[2].([]byte); !ok || string(vv) != "3" {
			t.Fatal("not 3")
		}
	}

	if v, err := redis.Values(c.Do("mget", "a_unique", "nonexistent_unique", "c_unique")); err != nil {
		t.Fatal(err)
	} else if len(v) != 3 {
		t.Fatal(len(v))
	} else {
		if vv, ok := v[0].([]byte); !ok || string(vv) != "1" {
			t.Fatal("not 1")
		}

		if v[1] != nil {
			t.Fatal("should be nil")
		}

		if vv, ok := v[2].([]byte); !ok || string(vv) != "3" {
			t.Fatal("not 3")
		}
	}

	// Test DEL operations
	if ok, err := redis.String(c.Do("set", "delkey1_unique", "value1")); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}

	if ok, err := redis.String(c.Do("set", "delkey2_unique", "value2")); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}

	if n, err := redis.Int(c.Do("del", "delkey1_unique")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if _, err := redis.String(c.Do("get", "delkey1_unique")); err != redis.ErrNil {
		t.Fatal(err)
	}

	if n, err := redis.Int(c.Do("del", "delkey2_unique", "nonexistent_unique")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if _, err := redis.String(c.Do("get", "delkey2_unique")); err != redis.ErrNil {
		t.Fatal(err)
	}

	// Test EXISTS operations
	if n, err := redis.Int(c.Do("exists", "nonexistent_exists_unique")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if ok, err := redis.String(c.Do("set", "existstest_unique", "value")); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("exists", "existstest_unique")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Int(c.Do("del", "existstest_unique")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("exists", "existstest_unique")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	// Test APPEND operations
	appendKey := "appendtest_unique"
	c.Do("del", appendKey)

	if n, err := redis.Int(c.Do("append", appendKey, "Hello ")); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("append", appendKey, "World")); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if v, err := redis.String(c.Do("get", appendKey)); err != nil {
			t.Fatal(err)
		} else if v != "Hello World" {
			t.Fatalf("get fail exp:%s act:%s", "Hello World", v)
		}
	}

	// Test STRLEN operations
	strlenKey := "strlentest_unique"
	c.Do("del", strlenKey)

	if ok, err := redis.String(c.Do("set", strlenKey, "Hello World")); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("strlen", strlenKey)); err != nil {
			t.Fatal(err)
		} else if n != 11 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Int(c.Do("strlen", "nonexistent_strlen_unique")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	// Test GETRANGE/SETRANGE operations
	rangeKey := "rangertest_unique"
	c.Do("del", rangeKey)

	if n, err := redis.Int(c.Do("append", rangeKey, "Hello World")); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if v, err := redis.String(c.Do("getrange", rangeKey, 0, 4)); err != nil {
			t.Fatal(err)
		} else if v != "Hello" {
			t.Fatal(v)
		}

		if v, err := redis.String(c.Do("getrange", rangeKey, 6, 10)); err != nil {
			t.Fatal(err)
		} else if v != "World" {
			t.Fatal(v)
		}

		if v, err := redis.String(c.Do("getrange", rangeKey, 0, -1)); err != nil {
			t.Fatal(err)
		} else if v != "Hello World" {
			t.Fatal(v)
		}
	}

	if n, err := redis.Int(c.Do("setrange", rangeKey, 6, "Redis")); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if v, err := redis.String(c.Do("get", rangeKey)); err != nil {
			t.Fatal(err)
		} else if v != "Hello Redis" {
			t.Fatalf("get fail exp:%s act:%s", "Hello Redis", v)
		}
	}

	// Test GETSET operations
	getsetKey := "getsettest_unique"
	c.Do("del", getsetKey)

	if v, err := redis.String(c.Do("getset", getsetKey, "value1")); err != nil {
		if err != redis.ErrNil {
			t.Fatal(err)
		}
		// When key doesn't exist, GETSET returns nil, which translates to empty string
	} else if v != "" {
		t.Fatal(v)
	}

	if v, err := redis.String(c.Do("get", getsetKey)); err != nil {
		t.Fatal(err)
	} else if v != "value1" {
		t.Fatalf("get fail exp:%s act:%s", "value1", v)
	}

	if old, err := redis.String(c.Do("getset", getsetKey, "value2")); err != nil {
		t.Fatal(err)
	} else if old != "value1" {
		t.Fatal(old)
	}

	if v, err := redis.String(c.Do("get", getsetKey)); err != nil {
		t.Fatal(err)
	} else if v != "value2" {
		t.Fatalf("get fail exp:%s act:%s", "value2", v)
	}

	// Test EXPIRE operations
	expireKey := "expiretest_unique"
	c.Do("del", expireKey)

	if ok, err := redis.String(c.Do("set", expireKey, "value")); err != nil {
		t.Fatal(err)
	} else if ok != resp.ReplyOK {
		t.Fatal(ok)
	}

	if n, err := redis.Int64(c.Do("expire", expireKey, 10)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if ttl, err := redis.Int64(c.Do("ttl", expireKey)); err != nil {
			t.Fatal(err)
		} else if ttl <= 0 || ttl > 10 {
			t.Fatal(ttl)
		}
	}

	futureTime := time.Now().Unix() + 10
	if n, err := redis.Int64(c.Do("expireat", expireKey, futureTime)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("persist", expireKey)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if ttl, err := redis.Int64(c.Do("ttl", expireKey)); err != nil {
			t.Fatal(err)
		} else if ttl != -1 {
			t.Fatal(ttl)
		}
	}

	// Test ERROR operations
	if _, err := c.Do("get", "a_err_unique", "b", "c"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := c.Do("set", "a_err_unique", "b", "c"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := c.Do("getset", "a_err_unique", "b", "c"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := c.Do("setnx", "a_err_unique", "b", "c"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := c.Do("exists", "a_err_unique", "b"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := c.Do("incr", "a_err_unique", "b"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := c.Do("incrby", "a_err_unique"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := c.Do("decrby", "a_err_unique"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := c.Do("del"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("mset"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("mset", "a_err_unique", "b", "c"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("mget"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("expire"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("expire", "a_err_unique", "b"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("expireat"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("expireat", "a_err_unique", "b"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("ttl"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("persist"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("setex", "a_err_unique", "blah", "hello world"); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	// Test INCR/DECR operations
	incrdecKey := "incrdecrtest_unique"
	c.Do("del", incrdecKey)

	if n, err := redis.Int64(c.Do("incr", incrdecKey)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("incr", incrdecKey)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("decr", incrdecKey)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("incrby", incrdecKey, 10)); err != nil {
		t.Fatal(err)
	} else if n != 11 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("decrby", incrdecKey, 10)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	// Test INCRBYFLOAT operations
	incrfloatKey := "incrfloattest_unique"
	c.Do("del", incrfloatKey)

	if n, err := redis.String(c.Do("incrbyfloat", incrfloatKey, 10.50)); err != nil {
		t.Fatal(err)
	} else if n != "10.5" {
		t.Fatal(n)
	}

	if n, err := redis.String(c.Do("incrbyfloat", incrfloatKey, 0.1)); err != nil {
		t.Fatal(err)
	} else if n != "10.6" {
		t.Fatal(n)
	}

	if n, err := redis.String(c.Do("incrbyfloat", incrfloatKey, -5)); err != nil {
		t.Fatal(err)
	} else if n != "5.6" {
		t.Fatal(n)
	}

	if n, err := redis.String(c.Do("incrbyfloat", incrfloatKey, 0.000000001)); err != nil {
		t.Fatal(err)
	} else if n != "5.600000001" {
		t.Fatal(n)
	}

	c.Do("set", incrfloatKey, "5.0e3")
	if n, err := redis.String(c.Do("incrbyfloat", incrfloatKey, 2.0e2)); err != nil {
		t.Fatal(err)
	} else if n != "5200" {
		t.Fatal(n)
	}

	// Test concurrency operations
	var wg sync.WaitGroup
	var kid atomic.Uint64

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			conn := getTestConnWithAddr(testDBPort)
			defer func() {
				conn.Close()
				wg.Done()
			}()
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("TestStringCmds_concurrent_%d", kid.Add(1))
				if ok, err := redis.String(conn.Do("set", key, key)); err != nil {
					t.Error(err)
					return
				} else if ok != resp.ReplyOK {
					t.Errorf("Expected OK, got %s", ok)
					return
				}
			}
		}()
	}
	wg.Wait()

	concurrentCount := int(kid.Load())
	for i := 1; i <= concurrentCount; i++ {
		key := fmt.Sprintf("TestStringCmds_concurrent_%d", i)
		for rep := 0; rep < readNum; rep++ {
			if v, err := redis.String(c.Do("get", key)); err != nil {
				t.Fatal(err)
			} else if v != key {
				t.Fatalf("get fail exp:%s act:%s", key, v)
			}
		}
	}
}
