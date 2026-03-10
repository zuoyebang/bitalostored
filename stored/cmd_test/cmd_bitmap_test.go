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

	"github.com/zuoyebang/bitalostored/stored/internal/errn"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
)

func TestBitmapCmds(t *testing.T) {
	closeServer, err := startServer(testDBConf, testDBPort)
	require.NoError(t, err)
	defer closeServer()

	time.Sleep(100 * time.Millisecond)

	c := getTestConnWithAddr(testDBPort)
	defer c.Close()

	// Test basic Bitmap64 operations
	bitKey := "bit64_key_base"
	c.Do("del", bitKey)

	pos := 7
	n, err := redis.Int(c.Do("setbit64", bitKey, pos, 1))
	require.NoError(t, err)
	require.Equal(t, 0, n)

	n, err = redis.Int(c.Do("setbit64", bitKey, pos, 1))
	require.NoError(t, err)
	require.Equal(t, 1, n)

	n, err = redis.Int(c.Do("getbit64", bitKey, pos))
	require.NoError(t, err)
	require.Equal(t, 1, n)

	n, err = redis.Int(c.Do("getbit64", bitKey, pos+1))
	require.NoError(t, err)
	require.Equal(t, 0, n)

	n, err = redis.Int(c.Do("bitpos64", bitKey, 1, 0, 100))
	require.NoError(t, err)
	require.Equal(t, 7, n)

	n, err = redis.Int(c.Do("bitcount64", bitKey, 0, -1))
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// Test Bitmap64 Exist operations
	bitKeyExist := "bit64_key_exist"
	c.Do("del", bitKeyExist)
	posExist := 7
	if n, err := redis.Int(c.Do("setbit64", bitKeyExist, posExist, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
	if n, err := redis.Int(c.Do("exists", bitKeyExist)); err != nil {
		t.Fatal(err)
	} else {
		require.Equal(t, 1, n)
	}
	if n, err := redis.Int(c.Do("del", bitKeyExist)); err != nil {
		t.Fatal(err)
	} else {
		require.Equal(t, 1, n)
	}
	if n, err := redis.Int(c.Do("exists", bitKeyExist)); err != nil {
		t.Fatal(err)
	} else {
		require.Equal(t, 0, n)
	}

	// Test Bitmap64 Expire operations
	bitKeyExpire := "bit64_key_expire"
	c.Do("del", bitKeyExpire)
	posExpire := 7
	if n, err := redis.Int(c.Do("setbit64", bitKeyExpire, posExpire, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("expire", bitKeyExpire, 10)); err != nil {
		t.Fatal(err)
	} else {
		require.Equal(t, 1, n)
	}

	if n, err := redis.Int(c.Do("ttl", bitKeyExpire)); err != nil {
		t.Fatal(err)
	} else {
		if n <= 0 || n > 10 {
			t.Fatal("ttl", n)
		}
	}

	newTtl := time.Now().Unix() + 10
	if n, err := redis.Int(c.Do("expireAt", bitKeyExpire, newTtl)); err != nil {
		t.Fatal(err)
	} else {
		require.Equal(t, 1, n)
	}

	if n, err := redis.Int(c.Do("ttl", bitKeyExpire)); err != nil {
		t.Fatal(err)
	} else {
		if n <= 0 || n > 10 {
			t.Fatal("ttl", n)
		}
	}
	if n, err := redis.Int(c.Do("pttl", bitKeyExpire)); err != nil {
		t.Fatal(err)
	} else {
		if n <= 0 || n > 10000 {
			t.Fatal("ttl", n)
		}
	}

	if _, err := redis.Int(c.Do("persist", bitKeyExpire)); err != nil {
		t.Fatal(err)
	}
	if n, err := redis.Int(c.Do("ttl", bitKeyExpire)); err != nil {
		t.Fatal(err)
	} else {
		require.Equal(t, -1, n)
	}

	newTtl = time.Now().Unix() - 10
	if n, err := redis.Int(c.Do("expireAt", bitKeyExpire, newTtl)); err != nil {
		t.Fatal(err)
	} else {
		require.Equal(t, 1, n)
	}
	if n, err := redis.Int(c.Do("ttl", bitKeyExpire)); err != nil {
		t.Fatal(err)
	} else {
		require.Equal(t, -2, n)
	}

	if _, err := redis.Int(c.Do("del", bitKeyExpire)); err != nil {
		t.Fatal(err)
	}
	if n, err := redis.Int(c.Do("ttl", bitKeyExpire)); err != nil {
		t.Fatal(err)
	} else {
		require.Equal(t, -2, n)
	}

	// Test Bitmap64 Wrong Type operations
	bitKeyWrongType := "bit64_key_wrongtype"
	c.Do("del", bitKeyWrongType)
	posWrongType := 7
	if n, err := redis.Int(c.Do("setbit64", bitKeyWrongType, posWrongType, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if _, err := c.Do("hlen", bitKeyWrongType); err != nil {
		if err.Error() != errn.ErrWrongType.Error() {
			t.Fatal(err)
		}
	}

	// Test Bitmap64 Write Read 20 Key operations
	num := 20
	posBit1 := 7
	prefix := "TestBit64WriteRead20Key"

	c.Do("del", "TestBit64WriteRead20Key_0") // Clean up any existing keys
	c.Do("del", "TestBit64WriteRead20Key_1")

	for index := 0; index < num; index++ {
		bitKey := fmt.Sprintf("%s_%d", prefix, index)
		_, err := c.Do("setbit64", bitKey, posBit1, 1)
		require.NoError(t, err)
	}

	for index := 0; index < num; index++ {
		bitKey := fmt.Sprintf("%s_%d", prefix, index)
		n, err := redis.Int(c.Do("getbit64", bitKey, posBit1))
		require.NoError(t, err)
		require.Equal(t, 1, n)
	}
}
