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
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
)

func TestZSetCmds(t *testing.T) {
	closeServer, err := startServer(testDBConf, testDBPort)
	require.NoError(t, err)
	defer closeServer()

	time.Sleep(100 * time.Millisecond)

	c := getTestConnWithAddr(testDBPort)
	defer c.Close()

	// Test ZSet operations
	key := []byte("myzset")
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

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("zkeyexists", key)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("zcard", key)); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("zscore", key, "a")); err != nil {
			t.Fatal(err)
		} else if n != 3 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("zscore", key, "b")); err != nil {
			t.Fatal(err)
		} else if n != 4 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Int(c.Do("zadd", key, 2, "a", 5, "c")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("zscore", key, "a")); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("zcard", key)); err != nil {
			t.Fatal(err)
		} else if n != 3 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("zrem", key, "a", "b")); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("zcard", key)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("zscore", key, "c")); err != nil {
			t.Fatal(err)
		} else if n != 5 {
			t.Fatal(n)
		}
	}

	if n, err := redis.Int(c.Do("zclear", key)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	// Test ZSet Count operations
	key = []byte("myzset2")
	c.Do("del", key)

	if n, err := redis.Int(c.Do("zadd", key, 1, "a", 2, "b", 3, "c", 4, "d", 5, "e")); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	for i := 0; i < readNum; i++ {
		if n, err := redis.Int(c.Do("zcount", key, 0, 3)); err != nil {
			t.Fatal(err)
		} else if n != 3 {
			t.Fatal(n)
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

		if n, err := redis.Int(c.Do("zcount", key, "-inf", "+inf")); err != nil {
			t.Fatal(err)
		} else if n != 5 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("zcount", key, "(2", 5)); err != nil {
			t.Fatal(err)
		} else if n != 3 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("zlexcount", key, "-", "+")); err != nil {
			t.Fatal(err)
		} else if n != 5 {
			t.Fatal(n)
		}
	}

	// Test ZSet Rank operations
	key = []byte("test_zset_rank_test")
	c.Do("del", key)
	if _, err := redis.Int(c.Do("zadd", key, 1, "a", 2, "b", 3, "c", 4, "d")); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < readNum; i++ {
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

	// Test ZSet Range Score operations
	key = []byte("test_zset_range")
	c.Do("del", key)
	if _, err := redis.Int(c.Do("zadd", key, 1, "a", 2, "b", 3, "c", 4, "d")); err != nil {
		t.Fatal(err)
	}

	if v, err := redis.Values(c.Do("zrangebyscore", key, 1, 4, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrangebyscore", key, 1, 4, "withscores", "limit", 1, 2)); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "b", 2, "c", 3); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrangebyscore", key, "-inf", "+inf", "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrangebyscore", key, "(1", "(4")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "b", "c"); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrangebyscore", key, 4, 1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrangebyscore", key, 4, 1, "withscores", "limit", 1, 2)); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "c", 3, "b", 2); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrangebyscore", key, "+inf", "-inf", "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrangebyscore", key, "(4", "(1")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "c", "b"); err != nil {
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
		if err := validateZSetRange(v, "a", "d"); err != nil {
			t.Fatal(err)
		}
	}

	// Test ZSet Range operations
	key = []byte("test_zset_range_rank")
	c.Do("del", key)
	if _, err := redis.Int(c.Do("zadd", key, 1, "a", 2, "b", 3, "c", 4, "d")); err != nil {
		t.Fatal(err)
	}

	if v, err := redis.Values(c.Do("zrange", key, 0, 3, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrange", key, 1, 4, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "b", 2, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrange", key, -2, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "c", 3, "d", 4); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrange", key, 0, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "a", 1, "b", 2, "c", 3, "d", 4); err != nil {
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
		if err := validateZSetRange(v, "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrange", key, 0, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "d", 4, "c", 3, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrange", key, 2, 3, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "b", 2, "a", 1); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrange", key, -2, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "b", 2, "a", 1); err != nil {
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
		if err := validateZSetRange(v, "a", "b"); err != nil {
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

	// Test ZSet Lex operations
	key = []byte("myzlexset")
	c.Do("del", key)
	if n, err := redis.Int(c.Do("zadd", key, 0, "a", 0, "b", 0, "c", 0, "d", 0, "e")); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

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

	// Test error parameter operations
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

func TestZSetFloatCmds(t *testing.T) {
	closeServer, err := startServer(testDBConf, testDBPort)
	require.NoError(t, err)
	defer closeServer()

	time.Sleep(1 * time.Second)

	c := getTestConnWithAddr(testDBPort)
	defer c.Close()

	// Test ZSetFloat
	key := []byte("myzsetfloat")
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

	// Test ZSetFloatCount
	keyCount := []byte("myzsetfloatcount")
	c.Do("del", keyCount)
	if n, err := redis.Int(c.Do("zadd", keyCount, 1.0, "a", 2.0, "b", 3.0, "c", 4.0, "d", 5.0, "e")); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("zcount", keyCount, "-inf", "+inf")); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("zcount", keyCount, "(1", "5")); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("zcount", keyCount, "(1", "(5")); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("zcount", keyCount, "2", "4")); err != nil {
		t.Fatal(err)
	} else if n != 3 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("zcount", keyCount, "(2", "(4")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("zcount", keyCount, "(4", "(5")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("zcount", keyCount, "5", "10")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("zcount", keyCount, "6", "10")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("zcount", keyCount, "-inf", "5")); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("zcount", keyCount, "-inf", "(5")); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("zcount", keyCount, "1", "+inf")); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("zcount", keyCount, "(1", "+inf")); err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("zcount", keyCount, "10", "20")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	// Test ZSetFloatRank
	key = []byte("myzsetfloatrank")
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

	// Test ZSetFloatRangeScore
	key = []byte("myzsetfloatrangescore")
	c.Do("del", key)
	if _, err := redis.Int(c.Do("zadd", key, 1.21, "a", -2.11, "b", -3.22, "c", 4.111, "d", -1.1112, "e", -1.1111, "f", 0, "g", 1.1111, "h", 1.1112, "i")); err != nil {
		t.Fatal(err)
	}

	if v, err := redis.Values(c.Do("zrangebyscore", key, -3, 5, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "b", -2.11, "e", -1.1112, "f", -1.1111, "g", 0, "h", 1.1111, "i", 1.1112, "a", 1.21, "d", 4.111); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrangebyscore", key, -3, 5, "withscores", "limit", 1, 2)); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "e", -1.1112, "f", -1.1111); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrangebyscore", key, "-inf", "+inf", "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "c", -3.22, "b", -2.11, "e", -1.1112, "f", -1.1111, "g", 0, "h", 1.1111, "i", 1.1112, "a", 1.21, "d", 4.111); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrangebyscore", key, 5, -3, "withscores")); err != nil {
		t.Fatal(err)
	} else if err := validateZSetRange(v, "d", 4.111, "a", 1.21, "i", 1.1112, "h", 1.1111, "g", 0, "f", -1.1111, "e", -1.1112, "b", -2.11); err != nil {
		t.Fatal(err)
	}

	if v, err := redis.Values(c.Do("zrevrangebyscore", key, 5, -3, "withscores", "limit", 1, 2)); err != nil {
		t.Fatal(err)
	} else if err := validateZSetRange(v, "a", 1.21, "i", 1.1112); err != nil {
		t.Fatal(err)
	}

	if v, err := redis.Values(c.Do("zrevrangebyscore", key, "+inf", "-inf", "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "d", 4.111, "a", 1.21, "i", 1.1112, "h", 1.1111, "g", 0, "f", -1.1111, "e", -1.1112, "b", -2.11, "c", -3.22); err != nil {
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
		if err := validateZSetRange(v, "b", "e", "f", "g", "d"); err != nil {
			t.Fatal(err)
		}
	}

	// Test ZSetFloatRange
	key = []byte("myzsetfloatrange")
	c.Do("del", key)
	if _, err := redis.Int(c.Do("zadd", key, 1.21, "a", -2.11, "b", -3.22, "c", 4.111, "d")); err != nil {
		t.Fatal(err)
	}

	if v, err := redis.Values(c.Do("zrange", key, 0, 3, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "c", -3.22, "b", -2.11, "a", 1.21, "d", 4.111); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrange", key, 1, 4, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "b", -2.11, "a", 1.21, "d", 4.111); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrange", key, -2, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "a", 1.21, "d", 4.111); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrange", key, 0, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "c", -3.22, "b", -2.11, "a", 1.21, "d", 4.111); err != nil {
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
		if err := validateZSetRange(v, "d", 4.111, "a", 1.21, "b", -2.11, "c", -3.22); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrange", key, 0, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "d", 4.111, "a", 1.21, "b", -2.11, "c", -3.22); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrange", key, 2, 3, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "b", -2.11, "c", -3.22); err != nil {
			t.Fatal(err)
		}
	}

	if v, err := redis.Values(c.Do("zrevrange", key, -2, -1, "withscores")); err != nil {
		t.Fatal(err)
	} else {
		if err := validateZSetRange(v, "b", -2.11, "c", -3.22); err != nil {
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
		if err := validateZSetRange(v, "c", "b"); err != nil {
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

	// Test ZsetFloatErrorParams
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

	// Test ZSetFloatLex
	keyLex := []byte("myzlexset")
	c.Do("del", keyLex)
	if _, err := c.Do("zadd", keyLex,
		-1.1, "a", -1.1, "b", -1.1, "c", -1.1, "d", -1.1, "e", -1.1, "f", -1.1, "g"); err != nil {
		t.Fatal(err)
	}

	if ay, err := redis.Strings(c.Do("zrangebylex", keyLex, "-", "[c")); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ay, []string{"a", "b", "c"}) {
		t.Fatal("must equal")
	}

	if ay, err := redis.Strings(c.Do("zrangebylex", keyLex, "-", "(c")); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ay, []string{"a", "b"}) {
		t.Fatal("must equal")
	}

	if ay, err := redis.Strings(c.Do("zrangebylex", keyLex, "[aaa", "(g")); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ay, []string{"b", "c", "d", "e", "f"}) {
		t.Fatal("must equal")
	}

	if n, err := redis.Int64(c.Do("zlexcount", keyLex, "-", "(c")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("zremrangebylex", keyLex, "[aaa", "(g")); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal(n)
	}

	if n, err := redis.Int64(c.Do("zlexcount", keyLex, "-", "+")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal(n)
	}
}

func validateZSetRange(ay []interface{}, checkValues ...interface{}) error {
	if len(ay) != len(checkValues) {
		return fmt.Errorf("invalid return number %d != %d", len(ay), len(checkValues))
	}

	for i := 0; i < len(ay); i++ {
		v, ok := ay[i].([]byte)
		if !ok {
			// Handle the case where the value might be a float64 (for scores)
			if floatVal, ok := ay[i].(float64); ok {
				// Convert to string and compare with expected value
				floatStr := fmt.Sprintf("%g", floatVal) // %g removes trailing zeros
				switch cv := checkValues[i].(type) {
				case float64:
					if math.Abs(floatVal-cv) > 1e-9 { // Compare floats with tolerance
						return fmt.Errorf("not equal %f != %f", floatVal, cv)
					}
				case int:
					expectedFloat := float64(cv)
					if math.Abs(floatVal-expectedFloat) > 1e-9 {
						return fmt.Errorf("not equal %f != %f", floatVal, expectedFloat)
					}
				default:
					cvStr := fmt.Sprintf("%v", checkValues[i])
					if floatStr != cvStr {
						return fmt.Errorf("not equal %s != %s", floatStr, cvStr)
					}
				}
			} else {
				return fmt.Errorf("invalid data %d %v %T", i, ay[i], ay[i])
			}
			continue
		}

		switch cv := checkValues[i].(type) {
		case string:
			if string(v) != cv {
				return fmt.Errorf("not equal %s != %s", string(v), cv)
			}
		case float64:
			// Convert string to float and compare
			actualFloat, err := strconv.ParseFloat(string(v), 64)
			if err != nil {
				return fmt.Errorf("could not parse float from %s: %v", string(v), err)
			}
			if math.Abs(actualFloat-cv) > 1e-9 {
				return fmt.Errorf("not equal %f != %f", actualFloat, cv)
			}
		case int:
			// Convert string to int and compare
			actualInt, err := strconv.Atoi(string(v))
			if err != nil {
				return fmt.Errorf("could not parse int from %s: %v", string(v), err)
			}
			if actualInt != cv {
				return fmt.Errorf("not equal %d != %d", actualInt, cv)
			}
		default:
			cvStr := fmt.Sprintf("%v", checkValues[i])
			if string(v) != cvStr {
				return fmt.Errorf("not equal %s != %s", string(v), cvStr)
			}
		}
	}

	return nil
}
