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
	"testing"
	"time"

	"github.com/zuoyebang/bitalostored/stored/engine/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
)

func TestKeysCmds(t *testing.T) {
	closeServer, err := startServer(testDBConf, testDBPort)
	require.NoError(t, err)
	defer closeServer()

	time.Sleep(100 * time.Millisecond)

	c := getTestConnWithAddr(testDBPort)
	defer c.Close()

	// Test KeysCmd operations
	var key string
	expire := "expire"
	expireat := "expireat"
	pexpire := "pexpire"
	pexpireat := "pexpireat"
	ttl := "ttl"
	pttl := "pttl"
	persist := "persist"
	exists := "exists"

	for i := 0; i < 100; i++ {
		for _, tt := range btools.DataTypeNameList {
			key = fmt.Sprintf("test_keys_%s_%d", tt, i)
			switch tt {
			case btools.DataTypeStringName:
				if ok, err := redis.String(c.Do("set", key, "123")); err != nil {
					t.Fatal(err)
				} else if ok != resp.ReplyOK {
					t.Fatal(ok)
				}
			case btools.DataTypeListName:
				if n, err := redis.Int(c.Do("rpush", key, "123")); err != nil {
					t.Fatal(err)
				} else if n != 1 {
					t.Fatal(n)
				}
			case btools.DataTypeHashName:
				if n, err := redis.Int(c.Do("hset", key, "a", "123")); err != nil {
					t.Fatal(err)
				} else if n != 1 {
					t.Fatal(n)
				}
			case btools.DataTypeSetName:
				if n, err := redis.Int(c.Do("sadd", key, "123")); err != nil {
					t.Fatal(err)
				} else if n != 1 {
					t.Fatal(n)
				}
			case btools.DataTypeZsetName:
				if n, err := redis.Int(c.Do("zadd", key, 123, "a")); err != nil {
					t.Fatal(err)
				} else if n != 1 {
					t.Fatal(n)
				}
			}

			for i := 0; i < readNum; i++ {
				if tp, err := redis.String(c.Do("type", key)); err != nil {
					t.Fatal(err)
				} else if tp != tt {
					t.Fatal("type err", tp, tt)
				}

				if n, err := redis.Int(c.Do(exists, key)); err != nil {
					t.Fatal(err)
				} else if n != 1 {
					t.Fatal(n)
				}
			}

			if n, err := redis.Int(c.Do(expire, key, int64(10))); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}
			for i := 0; i < readNum; i++ {
				if tl, err := redis.Int64(c.Do(ttl, key)); err != nil {
					t.Fatal(err)
				} else if tl < 9 {
					t.Fatal("ttl err", tl)
				}
			}

			if n, err := redis.Int(c.Do(pexpire, key, int64(1900))); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}
			for i := 0; i < readNum; i++ {
				if tl, err := redis.Int64(c.Do(pttl, key)); err != nil {
					t.Fatal(err)
				} else if tl < 1890 {
					t.Fatal("pttl err", tl)
				}
			}

			if n, err := redis.Int(c.Do(expireat, key, tclock.GetTimestampSecond()+1000)); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}
			for i := 0; i < readNum; i++ {
				if tl, err := redis.Int64(c.Do(ttl, key)); err != nil {
					t.Fatal(err)
				} else if tl < 999 {
					t.Fatal("ttl err", tl)
				}
			}

			if n, err := redis.Int(c.Do(pexpireat, key, tclock.GetTimestampMilli()+1900)); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}
			for i := 0; i < readNum; i++ {
				if tl, err := redis.Int64(c.Do(pttl, key)); err != nil {
					t.Fatal(err)
				} else if tl < 1500 {
					t.Fatal("pttl err", tl)
				}
			}

			kErr := "not_exist_ttl"
			tm := tclock.GetTimestampSecond() + 1000
			if n, err := redis.Int(c.Do(expire, kErr, tm)); err != nil || n != 0 {
				t.Fatal(false)
			}
			if n, err := redis.Int(c.Do(expireat, kErr, tm)); err != nil || n != 0 {
				t.Fatal(false)
			}
			if n, err := redis.Int(c.Do(pexpire, kErr, tm)); err != nil || n != 0 {
				t.Fatal(false)
			}
			if n, err := redis.Int(c.Do(pexpireat, kErr, tm)); err != nil || n != 0 {
				t.Fatal(false)
			}
			for i := 0; i < readNum; i++ {
				if n, err := redis.Int(c.Do(ttl, kErr)); err != nil || n > -1 {
					t.Fatal(false)
				}
				if n, err := redis.Int(c.Do(pttl, kErr)); err != nil || n > -1 {
					t.Fatal(false)
				}
			}

			if n, err := redis.Int(c.Do(persist, key)); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}
			for i := 0; i < readNum; i++ {
				if n, err := redis.Int(c.Do(ttl, key)); err != nil {
					t.Fatal(err)
				} else if n != -1 {
					t.Fatal(n)
				}
			}
			if n, err := redis.Int(c.Do(expire, key, 0)); err != nil {
				t.Fatal(err)
			} else if n != 1 {
				t.Fatal(n)
			}
			for i := 0; i < readNum; i++ {
				if n, err := redis.Int(c.Do(exists, key)); err != nil {
					t.Fatal(err)
				} else if n != 0 {
					t.Fatal(n)
				}
			}
		}
	}

	// Test Keys WRONGTYPE operations
	func() {
		checkErrWrongType := func(err error) bool {
			return err.Error() == errn.ErrWrongType.Error()
		}

		key := "test_keys_wrongtype_zset"
		if n, err := redis.Int(c.Do("zadd", key, 123, "a")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("sadd", key, "a")); !checkErrWrongType(err) {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("hset", key, "a", "123")); !checkErrWrongType(err) {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("lpush", key, "a", "123")); !checkErrWrongType(err) {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if ok, err := redis.String(c.Do("set", key, "111")); err != nil {
			t.Fatal(err)
		} else if ok != resp.ReplyOK {
			t.Fatal(ok)
		}
		for i := 0; i < readNum; i++ {
			if v, err := redis.String(c.Do("get", key)); err != nil {
				t.Fatal(err)
			} else if v != "111" {
				t.Fatalf("get fail exp:%s act:%s", "111", v)
			}
		}

		if n, err := redis.Int(c.Do("zadd", key, 123, "a")); !checkErrWrongType(err) {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do("del", key)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
	}()

	// Test Keys Expire operations
	var (
		expireInner   string
		expireatInner string
		ttlInner      string
		persistInner  string
		keyInner      string
	)

	ttlType := []string{"k", "l", "h", "s", "z"}
	for _, tt := range ttlType {
		if tt == "k" {
			expireInner = "expire"
			expireatInner = "expireat"
			ttlInner = "ttl"
			persistInner = "persist"
		} else {
			expireInner = fmt.Sprintf("%sexpire", tt)
			expireatInner = fmt.Sprintf("%sexpireat", tt)
			ttlInner = fmt.Sprintf("%sttl", tt)
			persistInner = fmt.Sprintf("%spersist", tt)
		}

		switch tt {
		case "k":
			keyInner = "kv_ttl"
			c.Do("set", keyInner, "123")
		case "l":
			keyInner = "list_ttl"
			c.Do("rpush", keyInner, "123")
		case "h":
			keyInner = "hash_ttl"
			c.Do("hset", keyInner, "a", "123")
		case "s":
			keyInner = "set_ttl"
			c.Do("sadd", keyInner, "123")
		case "z":
			keyInner = "zset_ttl"
			c.Do("zadd", keyInner, 123, "a")
		}

		exp := int64(10)
		if n, err := redis.Int(c.Do(expireInner, keyInner, exp)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if tl, err := redis.Int64(c.Do(ttlInner, keyInner)); err != nil {
			t.Fatal(err)
		} else if tl <= -1 {
			t.Fatal("no ttl")
		}

		tm := tclock.GetTimestampSecond() + 1000
		if n, err := redis.Int(c.Do(expireatInner, keyInner, tm)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		for i := 0; i < readNum; i++ {
			if tl, err := redis.Int64(c.Do(ttlInner, keyInner)); err != nil {
				t.Fatal(err)
			} else if tl <= -1 {
				t.Fatal("no ttl")
			}
		}

		kErr := "not_exist_ttl"
		if n, err := redis.Int(c.Do(expireInner, kErr, tm)); err != nil || n != 0 {
			t.Fatal(false)
		}

		if n, err := redis.Int(c.Do(expireatInner, kErr, tm)); err != nil || n != 0 {
			t.Fatal(false)
		}

		for i := 0; i < readNum; i++ {
			if n, err := redis.Int(c.Do(ttlInner, kErr)); err != nil || n > -1 {
				t.Fatal(false)
			}
		}

		if n, err := redis.Int(c.Do(persistInner, keyInner)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		for i := 0; i < readNum; i++ {
			if n, err := redis.Int(c.Do(ttlInner, keyInner)); err != nil {
				t.Fatal(err)
			} else if n != -1 {
				t.Fatal(n)
			}
		}

		if n, err := redis.Int(c.Do(expireInner, keyInner, 10)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := redis.Int(c.Do(persistInner, keyInner)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		for i := 0; i < readNum; i++ {
			if n, err := redis.Int(c.Do(ttlInner, keyInner)); err != nil {
				t.Fatal(err)
			} else if n != -1 {
				t.Fatal(n)
			}
		}
	}
}
