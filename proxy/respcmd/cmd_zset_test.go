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

package respcmd

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/zuoyebang/bitalostored/proxy/resp"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func TestZSet(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("myzset")
	c.Do("del", key)

	n, err := redis.Int(c.Do("zadd", key, 3, "a", 4, "b"))
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	n, err = redis.Int(c.Do("zcard", key))
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	n, err = redis.Int(c.Do("zadd", key, 1, "a", 2, "b"))
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	n, err = redis.Int(c.Do("zcard", key))
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	n, err = redis.Int(c.Do("zadd", key, 3, "c", 0.5, "d"))
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	n, err = redis.Int(c.Do("zcard", key))
	assert.NoError(t, err)
	assert.Equal(t, 4, n)

	s, err := redis.Int(c.Do("zscore", key, "c"))
	assert.NoError(t, err)
	assert.Equal(t, 3, s)

	f, err := redis.Float64(c.Do("zscore", key, "d"))
	assert.NoError(t, err)
	assert.Equal(t, 0.5, f)

	n, err = redis.Int(c.Do("zrem", key, "d", "e"))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	time.Sleep(50 * time.Millisecond)
	n, err = redis.Int(c.Do("zcard", key))
	assert.NoError(t, err)
	assert.Equal(t, 3, n)

	n, err = redis.Int(c.Do("zincrby", key, 4, "c"))
	assert.NoError(t, err)
	assert.Equal(t, 7, n)

	n, err = redis.Int(c.Do("zincrby", key, -4, "c"))
	assert.NoError(t, err)
	assert.Equal(t, 3, n)

	n, err = redis.Int(c.Do("zincrby", key, 4, "d"))
	assert.NoError(t, err)
	assert.Equal(t, 4, n)

	time.Sleep(50 * time.Millisecond)
	n, err = redis.Int(c.Do("zcard", key))
	assert.NoError(t, err)
	assert.Equal(t, 4, n)

	n, err = redis.Int(c.Do("zrem", key, "a", "b", "c", "d"))
	assert.NoError(t, err)
	assert.Equal(t, 4, n)

	time.Sleep(100 * time.Millisecond)
	n, err = redis.Int(c.Do("zcard", key))
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

}

func TestZSetCount(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("myzset")
	c.Do("del", key)

	_, err := redis.Int(c.Do("zadd", key, 1, "a", 2, "b", 3, "c", 4, "d"))
	assert.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	n, err := redis.Int(c.Do("zcount", key, 2, 4))
	assert.NoError(t, err)
	assert.Equal(t, 3, n)

	n, err = redis.Int(c.Do("zcount", key, 4, 4))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	n, err = redis.Int(c.Do("zcount", key, 4, 3))
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	n, err = redis.Int(c.Do("zcount", key, "(2", 4))
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	n, err = redis.Int(c.Do("zcount", key, "2", "(4"))
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	n, err = redis.Int(c.Do("zcount", key, "(2", "(4"))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	n, err = redis.Int(c.Do("zcount", key, "-inf", "+inf"))
	assert.NoError(t, err)
	assert.Equal(t, 4, n)

	c.Do("zadd", key, 3, "e")

	time.Sleep(100 * time.Millisecond)
	n, err = redis.Int(c.Do("zcount", key, "(2", "(4"))
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	c.Do("zrem", key, "a", "b", "c", "d", "e")
}

func TestZSetRank(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key := []byte("myzset")
	c.Do("del", key)

	_, err := redis.Int(c.Do("zadd", key, 1, "a", 2, "b", 3, "c", 4, "d"))
	assert.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	n, err := redis.Int(c.Do("zrank", key, "c"))
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	_, err = redis.Int(c.Do("zrank", key, "e"))
	assert.Error(t, err)
	assert.Equal(t, redis.ErrNil, err)

	n, err = redis.Int(c.Do("zrevrank", key, "c"))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	_, err = redis.Int(c.Do("zrevrank", key, "e"))
	assert.Error(t, err)
	assert.Equal(t, redis.ErrNil, err)
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

	time.Sleep(50 * time.Millisecond)
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

	time.Sleep(50 * time.Millisecond)
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

	time.Sleep(50 * time.Millisecond)
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

	time.Sleep(50 * time.Millisecond)
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

}

func TestZsetErrorParams(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	_, err := c.Do("zadd", "test_zadd")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ZADD").Error(), err.Error())

	_, err = c.Do("zadd", "test_zadd", "a", "b", "c")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ZADD").Error(), err.Error())

	_, err = c.Do("zadd", "test_zadd", "-a", "a")
	assert.Error(t, err)
	assert.Equal(t, resp.FloatErr.Error(), err.Error())

	_, err = c.Do("zadd", "test_zad", "0.1", "a")
	assert.NoError(t, err)

	_, err = c.Do("zcard")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ZCARD").Error(), err.Error())

	_, err = c.Do("zscore", "test_zscore")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ZSCORE").Error(), err.Error())

	_, err = c.Do("zrem", "test_zrem")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ZREM").Error(), err.Error())

	_, err = c.Do("zincrby", "test_zincrby")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ZINCRBY").Error(), err.Error())

	_, err = c.Do("zincrby", "test_zincrby", 0.1, "a")
	assert.NoError(t, err)

	_, err = c.Do("zcount", "test_zcount")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ZCOUNT").Error(), err.Error())

	_, err = c.Do("zcount", "test_zcount", "-inf", "=inf")
	assert.Error(t, err)
	assert.Equal(t, resp.ValueErr.Error(), err.Error())

	_, err = c.Do("zcount", "test_zcount", "-inf", "+inf")
	assert.NoError(t, err)

	_, err = c.Do("zcount", "test_zcount", 0.1, 0.1)
	assert.NoError(t, err)

	_, err = c.Do("zrank", "test_zrank")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ZRANK").Error(), err.Error())

	_, err = c.Do("zrevrank", "test_zrevrank")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ZREVRANK").Error(), err.Error())

	_, err = c.Do("zremrangebyrank", "test_zremrangebyrank")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ZREMRANGEBYRANK").Error(), err.Error())

	_, err = c.Do("zremrangebyrank", "test_zremrangebyrank", 0.1, 0.1)
	assert.Error(t, err)
	assert.Equal(t, resp.ValueErr.Error(), err.Error())

	_, err = c.Do("zremrangebyscore", "test_zremrangebyscore")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ZREMRANGEBYSCORE").Error(), err.Error())

	_, err = c.Do("zremrangebyscore", "test_zremrangebyscore", "-inf", "a")
	assert.Error(t, err)
	assert.Equal(t, resp.ValueErr.Error(), err.Error())

	_, err = c.Do("zremrangebyscore", "test_zremrangebyscore", 0, "a")
	assert.Error(t, err)
	assert.Equal(t, resp.ValueErr.Error(), err.Error())

	_, err = c.Do("zrange", "test_zrange")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ZRANGE").Error(), err.Error())

	_, err = c.Do("zrange", "test_zrange", 0, 1, "withscore")
	assert.Error(t, err)
	assert.Equal(t, resp.SyntaxErr.Error(), err.Error())

	_, err = c.Do("zrange", "test_zrange", 0, 1, "withscores", "a")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ZRANGE").Error(), err.Error())

	_, err = c.Do("zrevrange", "test_zrevrange")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ZREVRANGE").Error(), err.Error())

	_, err = c.Do("zrangebyscore", "test_zrangebyscore")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ZRANGEBYSCORE").Error(), err.Error())

	_, err = c.Do("zrangebyscore", "test_zrangebyscore", 0, 1, "withscore")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ZRANGEBYSCORE").Error(), err.Error())

	_, err = c.Do("zrangebyscore", "test_zrangebyscore", 0, 1, "withscores", "limit")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ZRANGEBYSCORE").Error(), err.Error())

	_, err = c.Do("zrangebyscore", "test_zrangebyscore", 0, 1, "withscores", "limi", 1, 1)
	assert.Error(t, err)
	assert.Equal(t, resp.SyntaxErr.Error(), err.Error())

	_, err = c.Do("zrangebyscore", "test_zrangebyscore", 0, 1, "withscores", "limit", "a", 1)
	assert.Error(t, err)
	assert.Equal(t, resp.ValueErr.Error(), err.Error())

	_, err = c.Do("zrangebyscore", "test_zrangebyscore", 0, 1, "withscores", "limit", 1, "a")
	assert.Error(t, err)
	assert.Equal(t, resp.ValueErr.Error(), err.Error())

	_, err = c.Do("zrevrangebyscore", "test_zrevrangebyscore")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ZREVRANGEBYSCORE").Error(), err.Error())

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

	time.Sleep(50 * time.Millisecond)
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
