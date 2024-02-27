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
	"reflect"
	"strconv"
	"testing"

	"github.com/gomodule/redigo/redis"
)

func TestZSet(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("myzset")
	c.Do("del", key)
	if n, err := redis.Int(c.Do("zkeyexists", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zadd", key, 3, "a", 4, "b")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zkeyexists", key)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(n)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zadd", key, 1, "a", 2, "b")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(n)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zadd", key, 3, "c", 4, "d")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if s, err := redis.Int(c.Do("zscore", key, "c")); err != nil {
		t.Fatal(err)
	} else if s != 3 {
		t.Fatal(s)
	}

	if n, err := redis.Int(c.Do("zrem", key, "d", "e")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zincrby", key, 4, "c")); err != nil {
		t.Fatal(err)
	} else if n != 7 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zincrby", key, -4, "c")); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zincrby", key, 4, "d")); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zrem", key, "a", "b", "c", "d")); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

}

func TestZSetCount(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("myzset")
	c.Do("del", key)
	if _, err := redis.Int(c.Do("zadd", key, 1, "a", 2, "b", 3, "c", 4, "d")); err != nil {
		t.Fatal(err)
	}

	if n, err := redis.Int(c.Do("zcount", key, 2, 4)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zcount", key, 4, 4)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zcount", key, 4, 3)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zcount", key, "(2", 4)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zcount", key, "2", "(4")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zcount", key, "(2", "(4")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zcount", key, "-inf", "+inf")); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	c.Do("zadd", key, 3, "e")

	if n, err := redis.Int(c.Do("zcount", key, "(2", "(4")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	c.Do("zrem", key, "a", "b", "c", "d", "e")
}

func TestZSetRank(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("myzset_rank_test")
	c.Do("del", key)
	if _, err := redis.Int(c.Do("zadd", key, 1, "a", 2, "b", 3, "c", 4, "d")); err != nil {
		t.Fatal(err)
	}

	if n, err := redis.Int(c.Do("zrank", key, "c")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if _, err := redis.Int(c.Do("zrank", key, "e")); err != redis.ErrNil {
		t.Fatal(err)
	}

	if n, err := redis.Int(c.Do("zrevrank", key, "c")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if _, err := redis.Int(c.Do("zrevrank", key, "e")); err != redis.ErrNil {
		t.Fatal(err)
	}
}

func testZSetRange(ay []interface{}, checkValues ...interface{}) error {
	if len(ay) != len(checkValues) {
		return fmt.Errorf("invalid return number %d != %d", len(ay), len(checkValues))
	}

	for i := 0; i < len(ay); i++ {
		v, ok := ay[i].([]byte)
		if !ok {
			return fmt.Errorf("invalid data %d %v %T", i, ay[i], ay[i])
		}

		switch cv := checkValues[i].(type) {
		case string:
			if string(v) != cv {
				return fmt.Errorf("not equal %s != %s", v, checkValues[i])
			}
		default:
			if s, _ := strconv.Atoi(string(v)); s != checkValues[i] {
				return fmt.Errorf("not equal %s != %v", v, checkValues[i])
			}
		}

	}

	return nil
}

func TestZSetRangeScore(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("myzset_range")
	c.Do("del", key)
	if _, err := redis.Int(c.Do("zadd", key, 1, "a", 2, "b", 3, "c", 4, "d")); err != nil {
		t.Fatal(err)
	}

	if v, err := redis.Values(c.Do("zrangebyscore", key, 1, 4, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrangebyscore", key, 1, 4, "withscores", "limit", 1, 2)); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "b", 2, "c", 3); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrangebyscore", key, "-inf", "+inf", "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrangebyscore", key, "(1", "(4")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "b", "c"); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrangebyscore", key, 4, 1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrangebyscore", key, 4, 1, "withscores", "limit", 1, 2)); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "c", 3, "b", 2); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrangebyscore", key, "+inf", "-inf", "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrangebyscore", key, "(4", "(1")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "c", "b"); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := redis.Int(c.Do("zremrangebyscore", key, 2, 3)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if v, err := redis.Values(c.Do("zrangebyscore", key, 1, 4)); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "a", "d"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestZSetRange(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("myzset_range_rank")
	c.Do("del", key)

	if _, err := redis.Int(c.Do("zadd", key, 1, "a", 2, "b", 3, "c", 4, "d")); err != nil {
		t.Fatal(err)
	}

	if v, err := redis.Values(c.Do("zrange", key, 0, 3, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrange", key, 1, 4, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrange", key, -2, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrange", key, 0, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrange", key, -1, -2, "withscores")); err != nil {
		t.Fatal(err)
	} else if len(v) != 0 {
		t.Fatal(len(v))
	}

	if v, err := redis.Values(c.Do("zrevrange", key, 0, 4, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrange", key, 0, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrange", key, 2, 3, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrange", key, -2, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := redis.Int(c.Do("zremrangebyrank", key, 2, 3)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if v, err := redis.Values(c.Do("zrange", key, 0, 4)); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetRange(v, "a", "b"); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := redis.Int(c.Do("del", key)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

}

func TestZsetErrorParams(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	if _, err := c.Do("zadd", "test_zadd"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zadd", "test_zadd", "a", "b", "c"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zadd", "test_zadd", "-a", "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zadd", "test_zad", "0.1", "aaaa"); err != nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zcard"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zscore", "test_zscore"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrem", "test_zrem"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zincrby", "test_zincrby"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zincrby", "test_zincrby", 0.1, "a"); err != nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zcount", "test_zcount"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zcount", "test_zcount", "-inf", "=inf"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zcount", "test_zcount", 0.1, 0.2); err != nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrank", "test_zrank"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrevrank", "test_zrevrank"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zremrangebyrank", "test_zremrangebyrank"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zremrangebyrank", "test_zremrangebyrank", 0.1, 0.1); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zremrangebyscore", "test_zremrangebyscore"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zremrangebyscore", "test_zremrangebyscore", "-inf", "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zremrangebyscore", "test_zremrangebyscore", 0, "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrange", "test_zrange"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrange", "test_zrange", 0, 1, "withscore"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrange", "test_zrange", 0, 1, "withscores", "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrevrange", "test_zrevrange"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrangebyscore", "test_zrangebyscore"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrangebyscore", "test_zrangebyscore", 0, 1, "withscore"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrangebyscore", "test_zrangebyscore", 0, 1, "withscores", "limit"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrangebyscore", "test_zrangebyscore", 0, 1, "withscores", "limi", 1, 1); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrangebyscore", "test_zrangebyscore", 0, 1, "withscores", "limit", "a", 1); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrangebyscore", "test_zrangebyscore", 0, 1, "withscores", "limit", 1, "a"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zrevrangebyscore", "test_zrevrangebyscore"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("del"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zmclear"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zexpire", "test_zexpire"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zexpireat", "test_zexpireat"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zttl"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zpersist"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

}

func TestZSetLex(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("myzlexset")
	c.Do("del", key)
	if _, err := c.Do("zadd", key,
		0, "a", 0, "b", 0, "c", 0, "d", 0, "e", 0, "f", 0, "g"); err != nil {
		t.Fatal(err)
	}

	if ay, err := redis.Strings(c.Do("zrangebylex", key, "-", "[c")); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ay, []string{"a", "b", "c"}) {
		t.Fatal("must equal")
	}

	if ay, err := redis.Strings(c.Do("zrangebylex", key, "-", "(c")); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ay, []string{"a", "b"}) {
		t.Fatal("must equal")
	}

	if ay, err := redis.Strings(c.Do("zrangebylex", key, "[aaa", "(g")); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ay, []string{"b", "c", "d", "e", "f"}) {
		t.Fatal("must equal")
	}

	if n, err := redis.Int64(c.Do("zlexcount", key, "-", "(c")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("zremrangebylex", key, "[aaa", "(g")); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("zlexcount", key, "-", "+")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}
}
