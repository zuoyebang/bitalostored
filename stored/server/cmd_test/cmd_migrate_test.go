package cmd_test

import (
	"os"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

func testMigrateExpire(t *testing.T) {
	key := "h1"
	slotindex := utils.GetKeySlotId([]byte(key))

	from, to := _cluster(t)
	_check(t, from, "OK", "SET", key, "hello")
	_check(t, from, 1, "EXPIRE", key, 10)
	_check(t, from, 10, "TTL", key)
	_check(t, from, "OK", "migrateslots", "localhost", toCluster, slotindex)
	time.Sleep(100 * time.Millisecond)
	_check(t, from, "OK", "migrateend", slotindex)

	_check(t, from, -2, "TTL", key)
	_check(t, to, 10, "TTL", key)
}

func testMigrateString(t *testing.T) {
	key := "kv"
	slotindex := utils.GetKeySlotId([]byte(key))

	from, to := _cluster(t)
	_check(t, from, "OK", "SET", key, "hello")
	_check(t, from, "OK", "migrateslots", "localhost", toCluster, slotindex)
	_check(t, from, "OK", "migrateslots", "localhost", toCluster, slotindex)

	time.Sleep(100 * time.Millisecond)
	_check(t, from, "", "migratestatus", slotindex)
	_check(t, from, "OK", "migrateend", slotindex)

	_check(t, from, nil, "GET", key)
	_check(t, to, "hello", "GET", key)
}

func testMigrateMGet(t *testing.T) {
	key1, key2 := "h1", "1h"
	slot1 := utils.GetKeySlotId([]byte(key1))

	from, to := _cluster(t)
	_check(t, from, "OK", "SET", key1, "hello")
	_check(t, from, "OK", "SET", key2, "world")
	_check(t, from, "OK", "migrateslots", "localhost", toCluster, slot1)

	time.Sleep(100 * time.Millisecond)
	replay := _check(t, from, "", "MGET", key1, key2)
	res, ok := replay.([]interface{})
	if !ok {
		t.Error("MGET ERROR")
	}
	if len(res) != 2 || res[0] == nil || res[1] == nil {
		t.Error("MGET ERROR")
	}

	_check(t, from, "", "migratestatus", slot1)
	_check(t, from, "OK", "migrateend", slot1)

	replay = _check(t, from, "", "MGET", key1, key2)
	res, ok = replay.([]interface{})
	if !ok {
		t.Error("MGET ERROR")
	}
	if len(res) != 2 || res[1] == nil {
		t.Error("MGET ERROR")
	}
	replay = _check(t, to, "", "MGET", key1, key2)
	res, ok = replay.([]interface{})
	if !ok {
		t.Error("MGET ERROR")
	}
	if len(res) != 2 || res[0] == nil {
		t.Error("MGET ERROR")
	}
}

func testMigrateMSet(t *testing.T) {
	key1, key2 := "h1", "1h"
	slot1 := utils.GetKeySlotId([]byte(key1))

	from, to := _cluster(t)
	_check(t, from, "OK", "SET", key1, "hello")
	_check(t, from, "OK", "SET", key2, "world")
	_check(t, from, "OK", "migrateslots", "localhost", toCluster, slot1)

	time.Sleep(100 * time.Millisecond)
	_check(t, from, "OK", "MSET", key1, "hi", key2, "he")
	_check(t, from, "hi", "GET", key1)
	_check(t, from, "he", "GET", key2)

	_check(t, from, "", "migratestatus", slot1)
	_check(t, from, "OK", "migrateend", slot1)

	_check(t, to, "hi", "GET", key1)
}

func testMigrateHash(t *testing.T) {
	key := "hash"
	slotindex := utils.GetKeySlotId([]byte(key))

	from, to := _cluster(t)
	_check(t, from, "", "DEL", key)
	_check(t, from, 1, "HSET", key, "h1", "hello")
	_check(t, from, 1, "HSET", key, "h2", "world")
	_check(t, from, "OK", "migrateslots", "localhost", toCluster, slotindex)

	time.Sleep(100 * time.Millisecond)
	_check(t, from, "", "migratestatus", slotindex)
	_check(t, from, "OK", "migrateend", slotindex)

	_check(t, from, nil, "HGET", key, "h1")
	_check(t, to, "hello", "HGET", key, "h1")
}

func testMigrateList(t *testing.T) {
	key := "list"
	slotindex := utils.GetKeySlotId([]byte(key))

	from, to := _cluster(t)
	_check(t, from, "", "DEL", key)
	_check(t, from, 1, "LPUSH", key, "1")
	_check(t, from, 2, "LPUSH", key, "1")
	_check(t, from, "OK", "migrateslots", "localhost", toCluster, slotindex)

	time.Sleep(100 * time.Millisecond)
	_check(t, from, "", "migratestatus", slotindex)
	_check(t, from, "OK", "migrateend", slotindex)

	_check(t, from, nil, "LINDEX", key, "1")
	_check(t, to, "1", "LINDEX", key, "1")
}

func testMigrateZSet(t *testing.T) {
	key := "zset"
	slotindex := utils.GetKeySlotId([]byte(key))

	from, to := _cluster(t)
	_check(t, from, "", "DEL", key)
	_check(t, from, 1, "ZADD", key, "1", "hello")
	_check(t, from, 1, "ZADD", key, "2", "world")
	_check(t, from, "OK", "migrateslots", "localhost", toCluster, slotindex)

	time.Sleep(100 * time.Millisecond)
	_check(t, from, "", "migratestatus", slotindex)
	_check(t, from, "OK", "migrateend", slotindex)

	_check(t, from, 0, "ZCARD", key)
	_check(t, to, 2, "ZCARD", key)
}

func testMigrateSet(t *testing.T) {
	key := "set"
	slotindex := utils.GetKeySlotId([]byte(key))

	from, to := _cluster(t)
	_check(t, from, "", "DEL", key)
	_check(t, from, 1, "SADD", key, "1")
	_check(t, from, 1, "SADD", key, "2")
	_check(t, from, "OK", "migrateslots", "localhost", toCluster, slotindex)

	time.Sleep(100 * time.Millisecond)
	_check(t, from, "", "migratestatus", slotindex)
	_check(t, from, "OK", "migrateend", slotindex)

	_check(t, from, 0, "SCARD", key)
	_check(t, to, 2, "SCARD", key)
}

var toCluster = "8191"
var fromCluster = "8291"

func _get_leader(t *testing.T, address string) redis.Conn {
	from, err := redis.Dial("tcp", address)
	if err != nil {
		t.Error(err)
	}

	if res, err := from.Do("info", "_leader_address"); err != nil {
		t.Error(err)
	} else {
		if s, e := redis.String(res, err); e != nil {
			t.Error(err)
		} else {
			t.Log("leader: ", s)
		}
	}
	return from
}
func _cluster(t *testing.T) (redis.Conn, redis.Conn) {
	if v := os.Getenv("from"); v != "" {
		fromCluster = v
	}
	from := _get_leader(t, "localhost:"+fromCluster)

	if v := os.Getenv("to"); v != "" {
		toCluster = v
	}
	to := _get_leader(t, "localhost:"+toCluster)

	return from, to
}
func _check(t *testing.T, conn redis.Conn, expect interface{}, cmd string, arg ...interface{}) interface{} {
	reply, err := conn.Do(cmd, arg...)
	if err != nil {
	}
	switch res := reply.(type) {
	case []uint8:
		reply = string(res)
	case int64:
		reply = int(res)
	case redis.Error:
		reply = string(res)
	}

	t.Logf("\r    %s: cmd: %v arg: %v res: %v %T", log.FileLine(2, 1), cmd, arg, reply, reply)
	if expect != "" && expect != reply {
		t.Errorf("\r    %s: \033[31merror cmd: %v\033[0m", log.FileLine(2, 1), cmd)
	}
	return reply
}
