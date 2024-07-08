package cmd_test

import (
	"fmt"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

var bitmapFlushEnable bool

func TestBitBase(t *testing.T) {
	c := getTestConn()
	defer c.Close()

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
		assert.Equal(t, 1, n)
	}
	if n, err := redis.Int(c.Do("getbit", bitKey, pos+1)); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, 0, n)
	}
	if n, err := redis.Int(c.Do("bitpos", bitKey, 1, 0, 100)); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, 7, n)
	}
	if n, err := redis.Int(c.Do("bitcount", bitKey, 0, 100)); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, 1, n)
	}
}

func TestBitStrlen(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	bitKey := "bit_key_strlen"
	c.Do("del", bitKey)

	pos := 7
	if n, err := redis.Int(c.Do("setbit", bitKey, pos, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
	if n, err := redis.Int(c.Do("strlen", bitKey)); err != nil || n <= 0 {
		t.Fatal(err, n)
	}
}

func TestBitExist(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	bitKey := "bit_key_exist"
	c.Do("del", bitKey)
	pos := 7
	if n, err := redis.Int(c.Do("setbit", bitKey, pos, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
	if n, err := redis.Int(c.Do("exists", bitKey)); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, 1, n)
	}
	if n, err := redis.Int(c.Do("del", bitKey)); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, 1, n)
	}
	if n, err := redis.Int(c.Do("exists", bitKey)); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, 0, n)
	}
}

func TestBitGetSet(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	bitKey := "bit_key_getset"
	c.Do("del", bitKey)
	pos := 7

	rb := roaring64.NewBitmap()
	rb.Add(uint64(pos))
	bin, _ := rb.MarshalBinary()

	if n, err := redis.Int(c.Do("setbit", bitKey, pos, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	newVal := "abc"
	if res, err := redis.Bytes(c.Do("getset", bitKey, newVal)); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, bin, res)
	}

	if res, err := redis.String(c.Do("get", bitKey)); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, newVal, res)
	}
}

func TestBitStringSetNx(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	bitKey := "bit_key_setnx"
	c.Do("del", bitKey)
	pos := 7
	if n, err := redis.Int(c.Do("setbit", bitKey, pos, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	newVal := "abc"
	if n, err := redis.Int(c.Do("setnx", bitKey, newVal)); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, 0, n)
	}
}

func TestBitStringExpire(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	bitKey := "bit_key_expire"
	c.Do("del", bitKey)
	pos := 7
	if n, err := redis.Int(c.Do("setbit", bitKey, pos, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if n, err := redis.Int(c.Do("expire", bitKey, 10)); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, 1, n)
	}

	if n, err := redis.Int(c.Do("ttl", bitKey)); err != nil {
		t.Fatal(err)
	} else {
		if n <= 0 || n > 10 {
			t.Fatal("ttl", n)
		}
	}

	newTtl := time.Now().Unix() + 10
	if n, err := redis.Int(c.Do("expireAt", bitKey, newTtl)); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, 1, n)
	}

	if n, err := redis.Int(c.Do("ttl", bitKey)); err != nil {
		t.Fatal(err)
	} else {
		if n <= 0 || n > 10 {
			t.Fatal("ttl", n)
		}
	}
	if n, err := redis.Int(c.Do("pttl", bitKey)); err != nil {
		t.Fatal(err)
	} else {
		if n <= 0 || n > 10000 {
			t.Fatal("ttl", n)
		}
	}

	if _, err := redis.Int(c.Do("persist", bitKey)); err != nil {
		t.Fatal(err)
	}
	if n, err := redis.Int(c.Do("ttl", bitKey)); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, -1, n)
	}

	newTtl = time.Now().Unix() - 10
	if n, err := redis.Int(c.Do("expireAt", bitKey, newTtl)); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, 1, n)
	}
	if n, err := redis.Int(c.Do("ttl", bitKey)); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, -2, n)
	}

	if _, err := redis.Int(c.Do("del", bitKey)); err != nil {
		t.Fatal(err)
	}
	if n, err := redis.Int(c.Do("ttl", bitKey)); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, -2, n)
	}
}

func TestBitWrongType(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	bitKey := "bit_key_wrongtype"
	c.Do("del", bitKey)
	pos := 7
	if n, err := redis.Int(c.Do("setbit", bitKey, pos, 1)); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}

	if _, err := (c.Do("hlen", bitKey)); err != nil {
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
	c := getTestConn()
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
	c := getTestConn()
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
	c := getTestConn()
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
	c := getTestConn()
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
	c := getTestConn()
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
