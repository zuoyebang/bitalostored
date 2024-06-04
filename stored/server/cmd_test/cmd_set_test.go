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
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
)

func TestSet(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key1 := "testdb_cmd_set_1"
	key2 := "testdb_cmd_set_2"
	c.Do("del", key1)
	c.Do("del", key2)
	if n, err := redis.Int(c.Do("skeyexists", key1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("sadd", key1, 0, 1)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("sadd", key1, 0, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("skeyexists", key1)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Int(c.Do("sadd", key2, 0, 1, 2, 3)); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("scard", key1)); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Int(c.Do("srem", key1, 0, 1)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("scard", key1)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("sismember", key2, 0)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := redis.Values(c.Do("smembers", key2)); err != nil {
			t.Fatal(err)
		} else if len(n) != 4 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Int(c.Do("sclear", key2)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	//time.Sleep(time.Second)

	if n, err := redis.Int(c.Do("sadd", key2, 1)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("sadd", key1, 0)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("sclear", key1, key2)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}
}

func TestSaddAndSclearAndSadd(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key2 := "saddsclarsadd"
	c.Do("del", key2)
	if n, err := redis.Int(c.Do("sadd", key2, 1)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("del", key2)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("sismember", key2, 1)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}

	//time.Sleep(time.Second)

	if n, err := redis.Int(c.Do("sadd", key2, 1)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
}

func TestSetErrorParams(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	if _, err := c.Do("sadd", "test_sadd"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("scard"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("scard", "k1", "k2"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sdiff"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sdiffstore", "dstkey"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sinter"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sinterstore", "dstkey"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sunion"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sunionstore", "dstkey"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sismember", "k1"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sismember", "k1", "m1", "m2"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("smembers"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("smembers", "k1", "k2"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("srem"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("srem", "key"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sclear"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sclear", "k1", "k2"); err != nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("smclear"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sexpire", "set_expire"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sexpire", "set_expire", "aaa"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sexpireat", "set_expireat"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sexpireat", "set_expireat", "aaa"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("sttl"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("spersist"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

}

func TestSetRandMember(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := "testdb_cmd_setranmember"
	nilkey := "testdb_cmd_nil_setranmember"
	c.Do("del", key)
	c.Do("del", nilkey)

	if n, err := redis.Int(c.Do("sadd", key, 0, 1, 2, 3)); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if _, err := redis.Values(c.Do("srandmember", key, 2, -1)); err == nil {
			t.Fatal(" err should not nil")
		} else if err.Error() != resp.ErrSyntax.Error() {
			t.Fatal(err)
		}

		if _, err := redis.Values(c.Do("srandmember", key, 1.3)); err == nil {
			t.Fatal(" err should not nil")
		} else if err.Error() != resp.ErrValue.Error() {
			t.Fatal(err)
		}

		if _, err := redis.String(c.Do("srandmember", key)); err != nil {
			t.Fatal(err)
		}

		if v, err := redis.Values(c.Do("srandmember", key, 0)); err != nil {
			t.Fatal(err)
		} else if len(v) != 0 {
			t.Fatal(len(v))
		}

		if v, err := redis.Values(c.Do("srandmember", key, 2)); err != nil {
			t.Fatal(err)
		} else if len(v) != 2 {
			t.Fatal(len(v))
		}

		if v, err := redis.Values(c.Do("srandmember", key, 7)); err != nil {
			t.Fatal(err)
		} else if len(v) != 4 {
			t.Fatal(len(v))
		}

		if v, err := redis.Values(c.Do("srandmember", key, -7)); err != nil {
			t.Fatal(err)
		} else if len(v) != 7 {
			t.Fatal(len(v))
		}

		if v, err := redis.Values(c.Do("srandmember", nilkey, 2)); err != nil {
			t.Fatal(err)
		} else if len(v) != 0 {
			t.Fatal(len(v))
		}
	}
}
