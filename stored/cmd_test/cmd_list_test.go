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

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
)

func testListIndex(key []byte, index int64, v int) error {
	c := getTestConnWithAddr(testDBPort)
	defer c.Close()

	for i := 0; i < readNum; i++ {
		n, err := redis.Int(c.Do("lindex", key, index))
		if err == redis.ErrNil && v != 0 {
			return fmt.Errorf("must nil")
		} else if err != nil && err != redis.ErrNil {
			return err
		} else if n != v {
			return fmt.Errorf("index err number %d != %d", n, v)
		}
	}

	return nil
}

func testListRange(key []byte, start int64, stop int64, checkValues ...int) error {
	c := getTestConnWithAddr(testDBPort)
	defer c.Close()

	for i := 0; i < readNum; i++ {
		vs, err := redis.Values(c.Do("lrange", key, start, stop))
		if err != nil {
			return err
		}

		if len(vs) != len(checkValues) {
			return fmt.Errorf("invalid return number %d != %d", len(vs), len(checkValues))
		}

		var n int
		for i, v := range vs {
			if d, ok := v.([]byte); ok {
				n, err = strconv.Atoi(string(d))
				if err != nil {
					return err
				} else if n != checkValues[i] {
					return fmt.Errorf("invalid data %d: %d != %d", i, n, checkValues[i])
				}
			} else {
				return fmt.Errorf("invalid data %v %T", v, v)
			}
		}
	}

	return nil
}

func TestListCmds(t *testing.T) {
	closeServer, err := startServer(testDBConf, testDBPort)
	require.NoError(t, err)
	defer closeServer()

	time.Sleep(100 * time.Millisecond)

	c := getTestConnWithAddr(testDBPort)
	defer c.Close()

	// Test basic list operations
	key := []byte("klist")
	c.Do("del", key)
	if n, err := redis.Int(c.Do("lkeyexists", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("lpush", key, 1)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("lkeyexists", key)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(1)
		}
	}

	if n, err := redis.Int(c.Do("rpush", key, 2)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("rpush", key, 3)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("llen", key)); err != nil {
			t.Fatal(err)
		} else if n != 3 {
			t.Fatal(n)
		}
	}

	if err := testListRange(key, 0, 0, 1); err != nil {
		t.Fatal(err)
	}

	if err := testListRange(key, 0, 1, 1, 2); err != nil {
		t.Fatal(err)
	}

	if err := testListRange(key, 0, 5, 1, 2, 3); err != nil {
		t.Fatal(err)
	}

	if err := testListRange(key, -1, 5, 3); err != nil {
		t.Fatal(err)
	}

	if err := testListRange(key, -5, -1, 1, 2, 3); err != nil {
		t.Fatal(err)
	}

	if err := testListRange(key, -2, -1, 2, 3); err != nil {
		t.Fatal(err)
	}

	if err := testListRange(key, -1, -2); err != nil {
		t.Fatal(err)
	}

	if err := testListRange(key, -1, 2, 3); err != nil {
		t.Fatal(err)
	}

	if err := testListRange(key, -5, 5, 1, 2, 3); err != nil {
		t.Fatal(err)
	}

	if err := testListRange(key, -1, 0); err != nil {
		t.Fatal(err)
	}

	if err := testListRange([]byte("empty list"), 0, 100); err != nil {
		t.Fatal(err)
	}

	if err := testListRange(key, -1, -1, 3); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(key, -1, 3); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(key, 0, 1); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(key, 1, 2); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(key, 2, 3); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(key, 5, 0); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(key, -1, 3); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(key, -2, 2); err != nil {
		t.Fatal(err)
	}

	if err := testListIndex(key, -3, 1); err != nil {
		t.Fatal(err)
	}

	// Test LRANGE operations
	keyLrange := "test_list_lrange"
	if _, delErr := c.Do("del", keyLrange); delErr != nil {
		t.Error("del error", delErr)
	}

	values := []string{"a", "b", "c", "d"}
	for _, v := range values {
		if _, err = c.Do("lpush", keyLrange, v); err != nil {
			t.Error("lpush error", err)
		}
	}

	var checkList [][3]int = [][3]int{
		{0, 0, 1},
		{0, 3, 4},
		{0, 4, 4},
		{0, 5, 4},
		{0, -1, 4},
		{0, -4, 1},
		{0, -5, 0},
		{-4, 0, 1},
		{-4, -1, 4},
		{-4, -4, 1},
		{-4, -5, 0},
		{-3, 1, 1},
		{-3, 10, 3},
		{-5, 0, 1},
		{-5, -1, 4},
		{-5, -4, 1},
		{-5, -5, 0},
		{-5, 5, 4},
		{3, 4, 1},
		{3, 3, 1},
		{3, 2, 0},
		{3, 0, 0},
		{3, -1, 1},
		{3, -2, 0},
		{3, -3, 0},
		{3, -4, 0},
		{3, -5, 0},
		{4, 0, 0},
		{5, 0, 0},
		{5, -1, 0},
		{5, -5, 0},
		{5, 5, 0},
	}

	for i := 0; i < readNum; i++ {
		for _, item := range checkList {
			start, stop, l := item[0], item[1], item[2]
			if r, _ := redis.Values(c.Do("lrange", keyLrange, start, stop)); len(r) != l {
				require.Equalf(t, l, len(r), "lrange with start=%d, stop=%d", start, stop)
			}
		}
		if _, err := redis.Values(c.Do("lrange", keyLrange, 0, 10000)); err != nil {
			t.Errorf("lrange error: %v", err)
		}

		if r, err := redis.Values(c.Do("lrange", "lrange_noexist_list", 0, 10000)); err != nil {
			t.Errorf("lrange error: %v", err)
		} else {
			require.Equalf(t, 0, len(r), "lrange on non-existent list")
		}
	}

	largeKey := "test_list_lrange_large"
	for i := 0; i <= 10000; i++ {
		c.Do("lpush", largeKey, i)
	}
	for i := 0; i < readNum; i++ {
		if r, err := redis.Values(c.Do("lrange", largeKey, 0, 9999)); err != nil {
			t.Errorf("lrange error: %v", err)
		} else {
			require.Equalf(t, 10000, len(r), "lrange on large list")
		}
		if r, err := redis.Values(c.Do("lrange", largeKey, 0, 10000)); err != nil {
			t.Errorf("lrange error: %v", err)
		} else {
			require.Equalf(t, 10000, len(r), "lrange on large list with extra range")
		}
	}
	c.Do("del", keyLrange)

	// Test multiple push operations
	keyMpush := []byte("list_mpush")
	c.Do("lclear", keyMpush)
	if n, err := redis.Int(c.Do("rpush", keyMpush, 1, 2, 3)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if err := testListRange(keyMpush, 0, 3, 1, 2, 3); err != nil {
		t.Fatal(err)
	}

	if n, err := redis.Int(c.Do("lpush", keyMpush, 1, 2, 3)); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if err := testListRange(keyMpush, 0, 6, 3, 2, 1, 1, 2, 3); err != nil {
		t.Fatal(err)
	}

	// Test POP operations
	keyPop := []byte("c")
	c.Do("del", keyPop)
	if n, err := redis.Int(c.Do("rpush", keyPop, 1, 2, 3, 4, 5, 6)); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if v, err := redis.Int(c.Do("lpop", keyPop)); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal(v)
	}

	if v, err := redis.Int(c.Do("rpop", keyPop)); err != nil {
		t.Fatal(err)
	} else if v != 6 {
		t.Fatal(v)
	}

	if n, err := redis.Int(c.Do("lpush", keyPop, 1)); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if err := testListRange(keyPop, 0, 5, 1, 2, 3, 4, 5); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		if v, err := redis.Int(c.Do("lpop", keyPop)); err != nil {
			t.Fatal(err)
		} else if v != i {
			t.Fatal(v)
		}
	}

	if n, err := redis.Int(c.Do("llen", keyPop)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	c.Do("rpush", keyPop, 1, 2, 3, 4, 5)

	if n, err := redis.Int(c.Do("lclear", keyPop)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("llen", keyPop)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}

	// Test TRIM operations
	keyTrim := []byte("dlist")
	c.Do("del", keyTrim)
	if n, err := redis.Int(c.Do("rpush", keyTrim, 1, 2, 3, 4, 5, 6)); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if ok, err := redis.String(c.Do("ltrim", keyTrim, 1, -1)); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatal(ok)
	}

	if n, err := redis.Int(c.Do("llen", keyTrim)); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if ok, err := redis.String(c.Do("ltrim", keyTrim, 2, 5)); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatal(ok)
	}

	if n, err := redis.Int(c.Do("llen", keyTrim)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if ok, err := redis.String(c.Do("ltrim", keyTrim, 2, 2)); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatal(ok)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("llen", keyTrim)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Int(c.Do("rpush", keyTrim, 1, 2)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("llen", keyTrim)); err != nil {
			t.Fatal(err)
		} else if n != 3 {
			t.Fatal(n)
		}
	}

	// Test error parameter operations
	if _, err := c.Do("lpush", "test_lpush"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("rpush", "test_rpush"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("lpop", "test_lpop", "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("rpop", "test_rpop", "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("llen", "test_llen", "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("lindex", "test_lindex"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("lrange", "test_lrange"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("lclear"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("lmclear"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("lexpire"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("lexpireat"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("lttl"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}
}
