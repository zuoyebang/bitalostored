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
	"math"
	"reflect"
	"strconv"
	"testing"

	"github.com/gomodule/redigo/redis"
)

func TestZSetFloat(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("myzsetfloat")
	c.Do("del", key)
	if n, err := redis.Int(c.Do("zkeyexists", key)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zadd", key, 3.0, "a", 4.0, "b")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
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
	}

	if n, err := redis.Int(c.Do("zadd", key, -1.0, "a", -2.0, "b")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("zcard", key)); err != nil {
			t.Fatal(n)
		} else if n != 2 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Int(c.Do("zadd", key, 3.0, "c", 4.0, "d")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("zcard", key)); err != nil {
			t.Fatal(err)
		} else if n != 4 {
			t.Fatal(n)
		}

		if s, err := redis.Float64(c.Do("zscore", key, "c")); err != nil {
			t.Fatal(err)
		} else if math.Abs(s-3.0) > math.SmallestNonzeroFloat64 {
			t.Fatal(s)
		}
	}

	if n, err := redis.Int(c.Do("zrem", key, "d", "e")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("zcard", key)); err != nil {
			t.Fatal(err)
		} else if n != 3 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Float64(c.Do("zincrby", key, 4.0, "c")); err != nil {
		t.Fatal(err)
	} else if math.Abs(n-7.0) > math.SmallestNonzeroFloat64 {
		t.Fatal(n)
	}

	if n, err := redis.Float64(c.Do("zincrby", key, -4, "c")); err != nil {
		t.Fatal(err)
	} else if math.Abs(n-3.0) > math.SmallestNonzeroFloat64 {
		t.Fatal(n)
	}

	if n, err := redis.Float64(c.Do("zincrby", key, 4.11, "d")); err != nil {
		t.Fatal(err)
	} else if math.Abs(n-4.11) > math.SmallestNonzeroFloat64 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("zcard", key)); err != nil {
			t.Fatal(err)
		} else if n != 4 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Int(c.Do("zrem", key, "a", "b", "c", "d")); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("zcard", key)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}

}

func TestZSetFloatCount(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("myzset")
	c.Do("del", key)
	if _, err := redis.Int(c.Do("zadd", key, -1.0, "a", 2.0, "b", -3.0, "c", 4.0, "d")); err != nil {
		t.Fatal(err)
	}

	if n, err := redis.Int(c.Do("zcount", key, 2, 4)); err != nil {
		t.Fatal(err)
	} else if n != 2 {
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
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zcount", key, "2", "(4")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zcount", key, "(2", "(4")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
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
	} else if n != 1 {
		t.Fatal(n)
	}

	c.Do("zrem", key, "a", "b", "c", "d", "e")
}

func TestZSetFloatRank(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("myzset_rank_test")
	c.Do("del", key)
	if _, err := redis.Int(c.Do("zadd", key, -1.111, "a", 2.21, "b", -3.13, "c", 4.01, "d")); err != nil {
		t.Fatal(err)
	}

	if n, err := redis.Int(c.Do("zrank", key, "c")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if _, err := redis.Int(c.Do("zrank", key, "e")); err != redis.ErrNil {
		t.Fatal(err)
	}

	if n, err := redis.Int(c.Do("zrevrank", key, "c")); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if _, err := redis.Int(c.Do("zrevrank", key, "e")); err != redis.ErrNil {
		t.Fatal(err)
	}
}

func testZSetFloatRange(ay []interface{}, checkValues ...interface{}) error {
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
		case float64:
			if num, e := strconv.ParseFloat(string(v), 64); e != nil || math.Abs(num-cv) > math.SmallestNonzeroFloat64 {
				return fmt.Errorf("not equal %s != %v", v, checkValues[i])
			}
		case int:
			if num, e := strconv.ParseFloat(string(v), 64); e != nil || math.Abs(num-float64(cv)) > math.SmallestNonzeroFloat64 {
				return fmt.Errorf("not equal %s != %v", v, checkValues[i])
			}
		default:
			return fmt.Errorf("not equal %s != %v", v, checkValues[i])
		}

	}

	return nil
}

func TestZSetFloatRangeScore(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("myzset_range")
	c.Do("del", key)
	if _, err := redis.Int(c.Do("zadd", key, 1.21, "a", -2.11, "b", -3.22, "c", 4.111, "d", -1.1112, "e", -1.1111, "f", 0, "g", 1.1111, "h", 1.1112, "i")); err != nil {
		t.Fatal(err)
	}

	if v, err := redis.Values(c.Do("zrangebyscore", key, -3, 5, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetFloatRange(v, "b", -2.11, "e", -1.1112, "f", -1.1111, "g", 0, "h", 1.1111, "i", 1.1112, "a", 1.21, "d", 4.111); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrangebyscore", key, -3, 5, "withscores", "limit", 1, 2)); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetFloatRange(v, "e", -1.1112, "f", -1.1111); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrangebyscore", key, "-inf", "+inf", "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetFloatRange(v, "c", -3.22, "b", -2.11, "e", -1.1112, "f", -1.1111, "g", 0, "h", 1.1111, "i", 1.1112, "a", 1.21, "d", 4.111); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrangebyscore", key, 5, -3, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetFloatRange(v, "d", 4.111, "a", 1.21, "i", 1.1112, "h", 1.1111, "g", 0, "f", -1.1111, "e", -1.1112, "b", -2.11); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrangebyscore", key, 5, -3, "withscores", "limit", 1, 2)); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetFloatRange(v, "a", 1.21, "i", 1.1112); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrangebyscore", key, "+inf", "-inf", "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetFloatRange(v, "d", 4.111, "a", 1.21, "i", 1.1112, "h", 1.1111, "g", 0, "f", -1.1111, "e", -1.1112, "b", -2.11, "c", -3.22); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := redis.Int(c.Do("zremrangebyscore", key, 1, 2)); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("zcard", key)); err != nil {
		t.Fatal(err)
	} else if n != 6 {
		t.Fatal(n)
	}

	if v, err := redis.Values(c.Do("zrangebyscore", key, -3, 5)); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetFloatRange(v, "b", "e", "f", "g", "d"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestZSetFloatRange(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("myzset_range_rank")
	c.Do("del", key)

	if _, err := redis.Int(c.Do("zadd", key, 1.21, "a", -2.11, "b", -3.22, "c", 4.111, "d")); err != nil {
		t.Fatal(err)
	}

	if v, err := redis.Values(c.Do("zrange", key, 0, 3, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetFloatRange(v, "c", -3.22, "b", -2.11, "a", 1.21, "d", 4.111); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrange", key, 1, 4, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetFloatRange(v, "b", -2.11, "a", 1.21, "d", 4.111); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrange", key, -2, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetFloatRange(v, "a", 1.21, "d", 4.111); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrange", key, 0, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetFloatRange(v, "c", -3.22, "b", -2.11, "a", 1.21, "d", 4.111); err != nil {
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
		if err := testZSetFloatRange(v, "d", 4.111, "a", 1.21, "b", -2.11, "c", -3.22); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrange", key, 0, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetFloatRange(v, "d", 4.111, "a", 1.21, "b", -2.11, "c", -3.22); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrange", key, 2, 3, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetFloatRange(v, "b", -2.11, "c", -3.22); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrange", key, -2, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := testZSetFloatRange(v, "b", -2.11, "c", -3.22); err != nil {
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
		if err := testZSetFloatRange(v, "c", "b"); err != nil {
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

	if _, err := redis.Int(c.Do("zadd", key, 1.21, "a")); err != nil {
		t.Fatal(err)
	}

	if n, err := redis.Int(c.Do("zremrangebyrank", key, 1, -1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if _, err := redis.Int(c.Do("zadd", key, -2.11, "b", -3.22, "c", 4.111, "d")); err != nil {
		t.Fatal(err)
	}

	if n, err := redis.Int(c.Do("zremrangebyrank", key, 100, -1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

}

func TestZsetFloatErrorParams(t *testing.T) {
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

	if _, err := c.Do("zcount", "test_zcount"); err == nil {
		t.Fatalf("invalid err of %v", err)
	}

	if _, err := c.Do("zcount", "test_zcount", "-inf", "=inf"); err == nil {
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

func TestZSetFloatLex(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("myzlexset")
	c.Do("del", key)
	if _, err := c.Do("zadd", key,
		-1.1, "a", -1.1, "b", -1.1, "c", -1.1, "d", -1.1, "e", -1.1, "f", -1.1, "g"); err != nil {
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
