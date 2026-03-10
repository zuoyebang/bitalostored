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

var bitmapFlushEnable bool

func TestStringBitCmds(t *testing.T) {
	closeServer, err := startServer(testDBConf, testDBPort)
	require.NoError(t, err)
	defer closeServer()

	time.Sleep(100 * time.Millisecond)

	c := getTestConnWithAddr(testDBPort)
	defer c.Close()

	// Test basic BIT operations
	bitKey := "bit_key_base"
	c.Do("del", bitKey)

	pos := 7
	if n, err := redis.Int(c.Do("setbit", bitKey, pos, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
	if n, err := redis.Int(c.Do("setbit", bitKey, pos, 1)); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	if n, err := redis.Int(c.Do("getbit", bitKey, pos)); err != nil {
		t.Fatal(err)
	} else {
		require.Equal(t, 1, n)
	}
	if n, err := redis.Int(c.Do("getbit", bitKey, pos+1)); err != nil {
		t.Fatal(err)
	} else {
		require.Equal(t, 0, n)
	}
	if n, err := redis.Int(c.Do("bitpos", bitKey, 1, 0, 100)); err != nil {
		t.Fatal(err)
	} else {
		require.Equal(t, 7, n)
	}
	if n, err := redis.Int(c.Do("bitcount", bitKey, 0, 100)); err != nil {
		t.Fatal(err)
	} else {
		require.Equal(t, 1, n)
	}

	// Test BITSTRLEN operations
	bitKeyStrlen := "bit_key_strlen"
	c.Do("del", bitKeyStrlen)

	posStrlen := 7
	if n, err := redis.Int(c.Do("setbit", bitKeyStrlen, posStrlen, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
	if n, err := redis.Int(c.Do("strlen", bitKeyStrlen)); err != nil || n <= 0 {
		t.Fatal(err, n)
	}

	// Test BITEXISTS operations
	bitKeyExists := "bit_key_exist"
	c.Do("del", bitKeyExists)
	posExists := 7
	if n, err := redis.Int(c.Do("setbit", bitKeyExists, posExists, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
	if n, err := redis.Int(c.Do("exists", bitKeyExists)); err != nil {
		t.Fatal(err)
	} else {
		require.Equal(t, 1, n)
	}
	if n, err := redis.Int(c.Do("del", bitKeyExists)); err != nil {
		t.Fatal(err)
	} else {
		require.Equal(t, 1, n)
	}
	if n, err := redis.Int(c.Do("exists", bitKeyExists)); err != nil {
		t.Fatal(err)
	} else {
		require.Equal(t, 0, n)
	}

	// Test BITSTRINGSETNX operations
	bitKeySetNx := "bit_key_setnx"
	c.Do("del", bitKeySetNx)
	posSetNx := 7
	if n, err := redis.Int(c.Do("setbit", bitKeySetNx, posSetNx, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	newVal := "abc"
	if n, err := redis.Int(c.Do("setnx", bitKeySetNx, newVal)); err != nil {
		t.Fatal(err)
	} else {
		require.Equal(t, 0, n)
	}

	// Test BITSTRINGEXPIRE operations
	bitKeyExpire := "bit_key_expire"
	c.Do("del", bitKeyExpire)
	posExpire := 7
	if n, err := redis.Int(c.Do("setbit", bitKeyExpire, posExpire, 1)); err != nil {
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

	// Test BITWRONGTYPE operations
	bitKeyWrongType := "bit_key_wrongtype"
	c.Do("del", bitKeyWrongType)
	posWrongType := 7
	if n, err := redis.Int(c.Do("setbit", bitKeyWrongType, posWrongType, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if _, err := c.Do("hlen", bitKeyWrongType); err != nil {
		if err.Error() != errn.ErrWrongType.Error() {
			t.Fatal(err)
		}
	}
}

func TestBitWriteRead20Key(t *testing.T) {
	if !bitmapFlushEnable {
		return
	}
	num := 20
	posBit1 := 7
	prefix := "TestBitWriteRead20Key"
	delSeqExec(prefix, num)
	setbitSeqExec(prefix, posBit1, num)
	getbitSeqExec(prefix, posBit1, num)
}

func TestBitWrite20ExpireKey(t *testing.T) {
	// set bitmapItemMax = 20
	// set bitmapFlushSecond = 60
	if !bitmapFlushEnable {
		return
	}

	prefix := "TestBitWrite20ExpireKey"
	n := 20
	pos := 7
	delSeqExec(prefix, n)

	// check log manually: bitmap item flush. expireNum:20 nullNum:0 flushNum:0
	c := getTestConnWithAddr(testDBPort)
	defer c.Close()
	for i := 0; i < n; i++ {
		bitKey := getBitKey(prefix, i)
		c.Do("setbit", bitKey, pos, 1)
		c.Do("expire", bitKey, 1)
	}
}

func TestBitWrite20EmptyKey(t *testing.T) {
	// set bitmapItemMax = 20
	// set bitmapFlushSecond = 60
	if !bitmapFlushEnable {
		return
	}

	prefix := "TestBitWrite20EmptyKey"
	n := 20
	pos := 7

	// check log manually: bitmap item flush. expireNum:0 nullNum:20 flushNum:0
	c := getTestConnWithAddr(testDBPort)
	defer c.Close()
	for i := 0; i < n; i++ {
		bitKey := getBitKey(prefix, i)
		c.Do("setbit", bitKey, pos, 1)
		c.Do("setbit", bitKey, pos, 0)
	}
}

func TestBitEvictPolicy(t *testing.T) {
	// set bitmapItemMax = 20
	// set bitmapFlushSecond = 60
	if !bitmapFlushEnable {
		return
	}

	n := 20
	pos := 7
	prefix := "TestBitEvictPolicy"
	delSeqExec(prefix, n)
	setbitSeqExec(prefix, pos, n)

	fmt.Println("wait 60 seconds...")
	time.Sleep(60 * time.Second)
	// check log manually: bitmap evict itemNum:6

	// check get ok
	getbitSeqExec(prefix, pos, n)
}

func delSeqExec(prefix string, num int) {
	c := getTestConnWithAddr(testDBPort)
	defer c.Close()
	for index := 0; index < num; index++ {
		bitKey := getBitKey(prefix, index)
		_, err := c.Do("del", bitKey)
		if err != nil {
			fmt.Printf("del key:%s err:%s", bitKey, err)
		}
	}
}

func setbitSeqExec(prefix string, pos, num int) {
	c := getTestConnWithAddr(testDBPort)
	defer c.Close()
	for index := 0; index < num; index++ {
		bitKey := getBitKey(prefix, index)
		_, err := c.Do("setbit", bitKey, pos, 1)
		if err != nil {
			fmt.Printf("setbit key:%s err:%s", bitKey, err)
		}
	}
}

func getbitSeqExec(prefix string, pos, num int) {
	c := getTestConnWithAddr(testDBPort)
	defer c.Close()
	for index := 0; index < num; index++ {
		bitKey := getBitKey(prefix, index)
		n, err := redis.Int(c.Do("getbit", bitKey, pos))
		if n != 1 || err != nil {
			fmt.Printf("key:%s pos:%d expect:1 actual:%d err:%s", bitKey, pos, 1, err)
		}
	}
}

func getBitKey(prefix string, index int) string {
	return fmt.Sprintf("%s_%d", prefix, index)
}
