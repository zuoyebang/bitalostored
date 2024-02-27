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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/zuoyebang/bitalostored/stored/internal/resp"

	"github.com/gomodule/redigo/redis"
)

func TestExpireKey(t *testing.T) {
	wg := sync.WaitGroup{}
	c := getTestConn()
	key := []byte("aaaa")
	c.Do("set", key, key)
	defer c.Close()
	for j := 0; j < 10; j++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 1000; i++ {
				c := getTestConn()
				if ttl, err := redis.Int(c.Do("ttl", key)); err != nil {
					t.Log("err:", err)
				} else if ttl < 1 {
					if ok, err := redis.Bool(c.Do("expire", key, 72000)); err != nil || !ok {
						t.Logf("err:%v, ok:%v", err, ok)
					}
				}
				c.Close()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestInfo(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	if res, err := redis.String(c.Do("info")); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("%+v", res)
	}
}

func TestCompact(t *testing.T) {
	for i := 0; i < 100; i++ {
		c := getTestConn()
		key := "TestCompactkey_" + strconv.Itoa(i)
		c.Do("del", "string"+key, "set"+key, "zset"+key, "hash"+key, "list"+key)

		if ok, err := redis.String(c.Do("set", "string"+key, "hello world")); err != nil {
			t.Fatal(err)
		} else if ok != resp.ReplyOK {
			t.Fatal(ok)
		}

		if n, err := redis.Int(c.Do("sadd", "set"+key, 0, 1)); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("zadd", "zset"+key, 3, "a", 4, "b")); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("hset", "hash"+key, 1, 0)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("lpush", "list"+key, 1)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if i >= 0 && i < 20 {
			if _, err := c.Do("expire", "string"+key, 1); err != nil {
				t.Fatalf("invalid err of %v", err)
			}
			if _, err := c.Do("expire", "set"+key, 1); err != nil {
				t.Fatalf("invalid err of %v", err)
			}
			if _, err := c.Do("expire", "zset"+key, 1); err != nil {
				t.Fatalf("invalid err of %v", err)
			}
			if _, err := c.Do("expire", "hash"+key, 1); err != nil {
				t.Fatalf("invalid err of %v", err)
			}
			if _, err := c.Do("expire", "list"+key, 1); err != nil {
				t.Fatalf("invalid err of %v", err)
			}
		}

		c.Close()
	}

	time.Sleep(5 * time.Second)
	c := getTestConn()
	defer c.Close()

	if _, err := redis.String(c.Do("compact")); err != nil {
		t.Fatal(err)
	}
}

func TestSetExpireData(t *testing.T) {
	for i := 0; i < 100; i++ {
		c := getTestConn()
		key := "TestSetExpireData1key_" + strconv.Itoa(i)
		c.Do("del", "string"+key, "set"+key, "zset"+key, "hash"+key, "list"+key)

		if ok, err := redis.String(c.Do("set", "string"+key, "hello world")); err != nil {
			t.Fatal(err)
		} else if ok != resp.ReplyOK {
			t.Fatal(ok)
		}

		if n, err := redis.Int(c.Do("sadd", "set"+key, 0, 1)); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("zadd", "zset"+key, 3, "a", 4, "b")); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("hset", "hash"+key, 1, 0)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("lpush", "list"+key, 1)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if i >= 0 && i < 20 {
			if _, err := c.Do("expire", "string"+key, 1); err != nil {
				t.Fatalf("invalid err of %v", err)
			}
			if _, err := c.Do("expire", "set"+key, 1); err != nil {
				t.Fatalf("invalid err of %v", err)
			}
			if _, err := c.Do("expire", "zset"+key, 1); err != nil {
				t.Fatalf("invalid err of %v", err)
			}
			if _, err := c.Do("expire", "hash"+key, 1); err != nil {
				t.Fatalf("invalid err of %v", err)
			}
			if _, err := c.Do("expire", "list"+key, 1); err != nil {
				t.Fatalf("invalid err of %v", err)
			}
		}

		c.Close()
	}
}

func TestWrongType(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("same-key")
	c.Do("del", key)
	if _, err := c.Do("hset", key, "a", "a"); err != nil {
		t.Fatalf("hset err %v", err)
		return
	}
	if _, err := c.Do("del", key); err != nil {
		t.Fatalf("del err %v", err)
		return
	}
	if _, err := c.Do("rpush", key, 1); err != nil {
		t.Fatalf("rpush err %v", err)
	}
}
