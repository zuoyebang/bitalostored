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
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func testListIndex(key []byte, index int64, v int) error {
	c := getTestConn()
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
	c := getTestConn()
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

func TestList(t *testing.T) {
	c := getTestConn()
	defer c.Close()

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
}

func TestListLrange(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := "test_list_lrange"
	_, err := c.Do("del", key)
	if err != nil {
		t.Error("del error", err)
		return
	}

	values := []string{"a", "b", "c", "d"}
	for _, v := range values {
		if _, err = c.Do("lpush", key, v); err != nil {
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
			if r, _ := redis.Values(c.Do("lrange", key, start, stop)); len(r) != l {
				assert.Equalf(t, 1, len(r), "lrange", start, stop)
			}
		}
		if _, err := redis.Values(c.Do("lrange", key, 0, 10000)); true {
			assert.Equalf(t, err, nil, "lrange")
		}

		if r, err := redis.Values(c.Do("lrange", "lrange_noexist_list", 0, 10000)); true {
			assert.Equalf(t, err, nil, "lrange")
			assert.Equalf(t, 0, len(r), "lrange")
		}
	}

	largeKey := "test_list_lrange_large"
	for i := 0; i <= 10000; i++ {
		c.Do("lpush", largeKey, i)
	}
	for i := 0; i < readNum; i++ {
		if r, err := redis.Values(c.Do("lrange", largeKey, 0, 9999)); true {
			assert.Equalf(t, err, nil, "lrange")
			assert.Equalf(t, 10000, len(r), "lrange")
		}
		if r, err := redis.Values(c.Do("lrange", largeKey, 0, 10000)); true {
			assert.Equalf(t, err, nil, "lrange")
			assert.Equalf(t, 10000, len(r), "lrange")
		}
	}
	c.Do("del", key)
}

func TestListMPush(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("list_mpush")
	c.Do("lclear", key)
	if n, err := redis.Int(c.Do("rpush", key, 1, 2, 3)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if err := testListRange(key, 0, 3, 1, 2, 3); err != nil {
		t.Fatal(err)
	}

	if n, err := redis.Int(c.Do("lpush", key, 1, 2, 3)); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if err := testListRange(key, 0, 6, 3, 2, 1, 1, 2, 3); err != nil {
		t.Fatal(err)
	}
}

func TestPop(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("c")
	c.Do("del", key)
	if n, err := redis.Int(c.Do("rpush", key, 1, 2, 3, 4, 5, 6)); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if v, err := redis.Int(c.Do("lpop", key)); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatal(v)
	}

	if v, err := redis.Int(c.Do("rpop", key)); err != nil {
		t.Fatal(err)
	} else if v != 6 {
		t.Fatal(v)
	}

	if n, err := redis.Int(c.Do("lpush", key, 1)); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if err := testListRange(key, 0, 5, 1, 2, 3, 4, 5); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		if v, err := redis.Int(c.Do("lpop", key)); err != nil {
			t.Fatal(err)
		} else if v != i {
			t.Fatal(v)
		}
	}

	if n, err := redis.Int(c.Do("llen", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	c.Do("rpush", key, 1, 2, 3, 4, 5)

	if n, err := redis.Int(c.Do("lclear", key)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("llen", key)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}

}

func TestTrim(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("dlist")
	c.Do("del", key)
	if n, err := redis.Int(c.Do("rpush", key, 1, 2, 3, 4, 5, 6)); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if ok, err := redis.String(c.Do("ltrim", key, 1, -1)); err != nil {
		t.Fatal(err)
	} else if ok != "OK" {
		t.Fatal(ok)
	}

	if n, err := redis.Int(c.Do("ltrim_front", key, 2)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("llen", key)); err != nil {
			t.Fatal(err)
		} else if n != 3 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Int(c.Do("ltrim_back", key, 2)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("llen", key)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Int(c.Do("ltrim_front", key, 5)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("llen", key)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Int(c.Do("rpush", key, 1, 2)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("ltrim_front", key, 2)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("llen", key)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}
}

func TestListErrorParams(t *testing.T) {
	c := getTestConn()
	defer c.Close()

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

	if _, err := c.Do("lpersist"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("ltrim_front", "test_ltrimfront", "-1"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("ltrim_back", "test_ltrimback", "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}
}
