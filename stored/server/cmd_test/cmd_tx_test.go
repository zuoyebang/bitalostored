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
	"bytes"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

func isSkipTestTx() bool {
	return skipTx
}

func TestTxMulti(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
}

func TestTxMultiNested(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if _, err := c.Do("multi"); err != nil {
		if err.Error() != errn.ErrMultiNested.Error() {
			t.Fatal(err)
		}
	}
}

func TestTxPrepareExecNoWatch(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	key := "a"
	val := "b"
	if _, err := redis.String(c.Do("set", key, val)); err != nil {
		t.Fatal(err)
	}

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}

	if res, err := c.Do("get", key); err != nil {
		t.Fatal(err)
	} else {
		r, _ := redis.String(res, err)
		if r != "QUEUED" {
			t.Fatalf("expect:QUEUED r:%+v", r)
		}
	}

	if res, err := redis.String(c.Do("prepare")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("prepare not ok", res)
		}
	}

	if res, err := redis.ByteSlices(c.Do("exec")); err != nil {
		t.Fatal(err)
	} else {
		if len(res) != 1 {
			t.Fatal("res len != 1", len(res))
		}
		if !bytes.Equal(res[0], []byte(val)) {
			t.Fatalf("res actual:%s expect:%s", res[0], val)
		}
	}
}

func TestTxPrepareDiscardNoWatch(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	key := "a"
	val := "b"
	if _, err := redis.String(c.Do("set", key, val)); err != nil {
		t.Fatal(err)
	}

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}

	if res, err := c.Do("get", key); err != nil {
		t.Fatal(err)
	} else {
		r, _ := redis.String(res, err)
		if r != "QUEUED" {
			t.Fatalf("expect:QUEUED r:%+v", r)
		}
	}

	if res, err := redis.String(c.Do("prepare")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("prepare not ok", res)
		}
	}

	if res, err := redis.String(c.Do("discard")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("discard is not ok", res)
		}
	}
}

func TestTxPrepareExecWatchNoChange(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	key := "a"
	val := "c"
	if _, err := redis.String(c.Do("set", key, val)); err != nil {
		t.Fatal(err)
	}

	if _, err := redis.String(c.Do("watch", key)); err != nil {
		t.Fatal(err)
	}

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}

	if res, err := c.Do("get", key); err != nil {
		t.Fatal(err)
	} else {
		r, _ := redis.String(res, err)
		if r != "QUEUED" {
			t.Fatalf("expect:QUEUED r:%+v", r)
		}
	}

	if res, err := redis.String(c.Do("prepare")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("prepare not ok", res)
		}
	}

	if res, err := redis.ByteSlices(c.Do("exec")); err != nil {
		t.Fatal(err)
	} else {
		if len(res) != 1 {
			t.Fatal("res len != 1", len(res))
		}
		if !bytes.Equal(res[0], []byte(val)) {
			t.Fatalf("res actual:%s expect:%s", res[0], val)
		}
	}
}

func TestTxWatch(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	key := "a"
	val := "c"
	if _, err := redis.String(c.Do("watch")); err != nil {
		if err.Error() != "ERR wrong number of arguments for 'watch' command" {
			t.Fatal(err)
		}
	}

	if _, err := redis.String(c.Do("set", key, val)); err != nil {
		t.Fatal(err)
	}

	if _, err := redis.String(c.Do("watch", key)); err != nil {
		t.Fatal(err)
	}

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}

	if res, err := c.Do("get", key); err != nil {
		t.Fatal(err)
	} else {
		r, _ := redis.String(res, err)
		if r != "QUEUED" {
			t.Fatalf("expect:QUEUED r:%+v", r)
		}
	}

	if res, err := redis.String(c.Do("prepare")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("prepare not ok", res)
		}
	}

	if res, err := redis.ByteSlices(c.Do("exec")); err != nil {
		t.Fatal(err)
	} else {
		if len(res) != 1 {
			t.Fatal("res len != 1", len(res))
		}
		if !bytes.Equal(res[0], []byte(val)) {
			t.Fatalf("res actual:%s expect:%s", res[0], val)
		}
	}
}

func TestTxWatchInMulti(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	key := "a"
	val := "c"

	if _, err := redis.String(c.Do("set", key, val)); err != nil {
		t.Fatal(err)
	}

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}

	if _, err := c.Do("watch", key); err != nil {
		if err.Error() != "ERR watch inside MULTI is not allowed" {
			t.Fatal(err)
		}
	}
}

func TestTxUnwatchInMulti(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	key := "a"
	val := "c"

	if _, err := redis.String(c.Do("set", key, val)); err != nil {
		t.Fatal(err)
	}

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}

	if _, err := c.Do("unwatch"); err != nil {
		if err.Error() != "ERR unwatch inside MULTI is not allowed" {
			t.Fatal(err)
		}
	}
}

func TestTxUnwatchFirst(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	key := "a"
	val := "c"

	for i := 0; i < 5; i++ {
		if _, err := c.Do("unwatch"); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := c.Do("watch", key); err != nil {
		t.Fatal(err)
	}

	if _, err := redis.String(c.Do("set", key, val)); err != nil {
		t.Fatal(err)
	}

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
}

func TestTxUnwatchBeforeMulti(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	key := "a"
	val := "c"

	if _, err := c.Do("watch", key); err != nil {
		t.Fatal(err)
	}

	if _, err := redis.String(c.Do("set", key, val)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do("unwatch"); err != nil {
		t.Fatal(err)
	}

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}

	if res, err := c.Do("get", key); err != nil {
		t.Fatal(err)
	} else {
		r, _ := redis.String(res, err)
		if r != "QUEUED" {
			t.Fatalf("expect:QUEUED r:%+v", r)
		}
	}

	if res, err := redis.String(c.Do("prepare")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("prepare not ok", res)
		}
	}

	if res, err := redis.ByteSlices(c.Do("exec")); err != nil {
		t.Fatal(err)
	} else {
		if len(res) != 1 {
			t.Fatal("res len != 1", len(res))
		}
		if !bytes.Equal(res[0], []byte(val)) {
			t.Fatalf("res actual:%s expect:%s", res[0], val)
		}
	}
}

func TestTxWatchAndSet(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	key := "a"
	val := "d"
	if _, err := redis.String(c.Do("set", key, val)); err != nil {
		t.Fatal(err)
	}

	if _, err := redis.String(c.Do("watch", key)); err != nil {
		t.Fatal(err)
	}

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}

	if res, err := c.Do("get", key); err != nil {
		t.Fatal(err)
	} else {
		r, _ := redis.String(res, err)
		if r != "QUEUED" {
			t.Fatalf("expect:QUEUED r:%+v", r)
		}
	}
	newVal := "e"
	if res, err := c.Do("set", key, newVal); err != nil {
		t.Fatal(err)
	} else {
		r, _ := redis.String(res, err)
		if r != "QUEUED" {
			t.Fatalf("expect:QUEUED r:%+v", r)
		}
	}
	if res, err := c.Do("get", key); err != nil {
		t.Fatal(err)
	} else {
		r, _ := redis.String(res, err)
		if r != "QUEUED" {
			t.Fatalf("expect:QUEUED r:%+v", r)
		}
	}

	if res, err := redis.String(c.Do("prepare")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("prepare not ok", res)
		}
	}

	if res, err := redis.Values(c.Do("exec")); err != nil {
		t.Fatal(res, err)
	} else {
		if len(res) != 3 {
			t.Fatal("res len != 3", len(res))
		}
		getv, ok := res[0].([]byte)
		if !ok {
			t.Fatal(res[0])
		}
		if !bytes.Equal(getv, []byte(val)) {
			t.Fatalf("res actual:%s expect:%s", getv, val)
		}
		getv, ok = res[2].([]byte)
		if !ok {
			t.Fatal(res[2])
		}
		if !bytes.Equal(getv, []byte(newVal)) {
			t.Fatalf("res actual:%s expect:%s", getv, newVal)
		}
	}
}

func TestTxPrepareWatchChange(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	key := "a"
	val := "d"
	if _, err := redis.String(c.Do("watch", key)); err != nil {
		t.Fatal(err)
	}
	if _, err := redis.String(c.Do("set", key, val)); err != nil {
		t.Fatal(err)
	}

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if _, err := c.Do("prepare"); err != nil {
		if err.Error() != errn.ErrWatchKeyChanged.Error() {
			t.Fatal(err)
		}
	}
}

func TestTxPrepare3KeyNoChange(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()
	c2 := getTestConn()
	defer c2.Close()

	otherWatchKey := "other-watch-key"
	if _, err := redis.String(c2.Do("watch", otherWatchKey)); err != nil {
		t.Fatal(err)
	}

	updateKey := "update-key"

	key := "a"
	val := "d"
	if _, err := redis.String(c.Do("watch", key)); err != nil {
		t.Fatal(err)
	}
	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if _, err := c.Do("set", key, val); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do("set", updateKey, val); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do("set", otherWatchKey, val); err != nil {
		t.Fatal(err)
	}
	if res, err := redis.String(c.Do("prepare")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("prepare not ok", res)
		}
	}
	if res, err := redis.Values(c.Do("exec")); err != nil {
		t.Fatal(err)
	} else {
		if len(res) != 3 {
			t.Fatal("res len not 3", len(res))
		}
	}
}

func TestTxPrepare3KeyOtherChange(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()
	c2 := getTestConn()
	defer c2.Close()

	otherWatchKey := "other-watch-key"
	if _, err := redis.String(c2.Do("watch", otherWatchKey)); err != nil {
		t.Fatal(err)
	}

	selfUpdateKey := "self-update-key"

	key := "a"
	val := "d"
	if _, err := redis.String(c.Do("watch", key)); err != nil {
		t.Fatal(err)
	}

	if _, err := c2.Do("set", otherWatchKey, val); err != nil {
		t.Fatal(err)
	}

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if _, err := c.Do("set", key, val); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do("set", selfUpdateKey, val); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do("set", otherWatchKey, val); err != nil {
		t.Fatal(err)
	}
	if res, err := redis.String(c.Do("prepare")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("prepare not ok", res)
		}
	}
	if res, err := redis.Values(c.Do("exec")); err != nil {
		t.Fatal(err)
	} else {
		if len(res) != 3 {
			t.Fatal("res len not 3", len(res))
		}
	}
}

func TestTxPrepareDeadlock(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c1 := getTestConn()
	defer c1.Close()
	c2 := getTestConn()
	defer c2.Close()

	c1WatchKey := "c1-watch-key"
	val := "d"
	if _, err := redis.String(c1.Do("watch", c1WatchKey)); err != nil {
		t.Fatal(err)
	}
	c2WatchKey := "c2-watch-key"
	if _, err := redis.String(c2.Do("watch", c2WatchKey)); err != nil {
		t.Fatal(err)
	}
	if res, err := redis.String(c1.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if _, err := c1.Do("set", c2WatchKey, val); err != nil {
		t.Fatal(err)
	}

	if res, err := redis.String(c2.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if _, err := c2.Do("set", c1WatchKey, val); err != nil {
		t.Fatal(err)
	}

	if res, err := redis.String(c1.Do("prepare")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("prepare not ok", res)
		}
	}
	if _, err := c2.Do("prepare"); err != nil {
		if err.Error() != errn.ErrPrepareLockFail.Error() {
			t.Fatal(err)
		}
	}
	if res, err := redis.Values(c1.Do("exec")); err != nil {
		t.Fatal(err)
	} else {
		if len(res) != 1 {
			t.Fatal("res len not 1", len(res))
		}
	}
	c2.Do("discard")
}

func TestTxReWatchAndChange(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	key := "a"
	val := "d"
	if _, err := redis.String(c.Do("watch", key)); err != nil {
		t.Fatal(err)
	}
	if _, err := redis.String(c.Do("set", key, val)); err != nil {
		t.Fatal(err)
	}
	if _, err := redis.String(c.Do("watch", key)); err != nil {
		t.Fatal(err)
	}

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if _, err := c.Do("prepare"); err != nil {
		if err.Error() != errn.ErrWatchKeyChanged.Error() {
			t.Fatal(err)
		}
	}
}

func TestTxDiscard(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	key := "a"
	val := "d"
	if _, err := redis.String(c.Do("set", key, val)); err != nil {
		t.Fatal(err)
	}
	if res, err := redis.String(c.Do("get", key)); err != nil {
		t.Fatal(err)
	} else {
		if res != val {
			t.Fatal("expect", val, "actual", res)
		}
	}
	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	newVal := "e"
	if res, err := c.Do("set", key, newVal); err != nil {
		t.Fatal(err)
	} else {
		r, _ := redis.String(res, err)
		if r != "QUEUED" {
			t.Fatalf("expect:QUEUED r:%+v", r)
		}
	}
	if res, err := redis.String(c.Do("prepare")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("prepare not ok", res)
		}
	}
	if res, err := redis.String(c.Do("discard")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("discard not ok", res)
		}
	}
	if res, err := redis.String(c.Do("get", key)); err != nil {
		t.Fatal(err)
	} else {
		if res != val {
			t.Fatal("expect", val, "actual", res)
		}
	}
}

func TestTxModifyByOtherClient(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c1 := getTestConn()
	defer c1.Close()

	c2 := getTestConn()
	defer c2.Close()

	key := "a"
	val := "d"
	if _, err := redis.String(c1.Do("set", key, val)); err != nil {
		t.Fatal(err)
	}
	if res, err := redis.String(c1.Do("watch", key)); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}

	newVal := "TestTxModifyByOtherClient-c2"
	if _, err := c2.Do("set", key, newVal); err != nil {
		t.Fatal(err)
	}

	if res, err := redis.String(c1.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("multi not ok", res)
		}
	}
	if _, err := c1.Do("get", key); err != nil {
		t.Fatal(err)
	}
	if _, err := c1.Do("prepare"); err != nil {
		if err.Error() != errn.ErrWatchKeyChanged.Error() {
			t.Fatal(err)
		}
	}
	if _, err := c1.Do("discard"); err != nil {
		if err.Error() != errn.ErrDiscardNoMulti.Error() {
			t.Fatal(err)
		}
	}

	if res, err := redis.String(c1.Do("get", key)); err != nil {
		t.Fatal(err)
	} else {
		if res != newVal {
			t.Fatal("expect", newVal, "actual", res)
		}
	}
}

func TestTxCloseClient(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	tryNum := 5

	c1 := getTestConn()
	defer c1.Close()

	key := "a"
	val := "x"
	if _, err := redis.String(c1.Do("set", key, val)); err != nil {
		t.Fatal(err)
	}

	multiClient := func() {
		c := getTestConn()
		defer c.Close()

		key := "a"
		val := "d"
		if res, err := redis.String(c.Do("watch", key)); err != nil {
			t.Fatal(err)
		} else {
			if res != "OK" {
				t.Fatal("res is not ok", res)
			}
		}
		if res, err := redis.String(c.Do("multi")); err != nil {
			t.Fatal(err)
		} else {
			if res != "OK" {
				t.Fatal("multi not ok", res)
			}
		}
		if _, err := c.Do("set", key, val); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Do("get", key); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Do("prepare"); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < tryNum; i++ {
		multiClient()
	}

	if res, err := redis.String(c1.Do("get", key)); err != nil {
		t.Fatal(err)
	} else {
		if res != val {
			t.Fatal("expect", val, "actual", res)
		}
	}
}

func TestTxPrepareNested(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if _, err := c.Do("prepare"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do("prepare"); err != nil {
		if err.Error() != errn.ErrPrepareNested.Error() {
			t.Fatal(err)
		}
	}
}

func TestTxPrepareWithoutMulti(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	if _, err := c.Do("prepare"); err != nil {
		if err.Error() != errn.ErrPrepareNoMulti.Error() {
			t.Fatal(err)
		}
	}
}

func TestTxMultiNoCommand(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}

	if res, err := redis.String(c.Do("prepare")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("prepare not ok", res)
		}
	}

	if res, err := redis.String(c.Do("exec")); err != nil {
		t.Fatal(res, err)
	} else {
		if res != "(empty array)" {
			t.Fatal("res expect:empty array", res)
		}
	}
}

func TestTxMultiCommand(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	if _, err := c.Do("set", "a", "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do("set", "b", "b"); err != nil {
		t.Fatal(err)
	}

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}

	if res, err := redis.String(c.Do("get", "a")); err != nil {
		t.Fatal(err)
	} else {
		if res != "QUEUED" {
			t.Fatal(res)
		}
	}
	if res, err := redis.String(c.Do("get", "b")); err != nil {
		t.Fatal(err)
	} else {
		if res != "QUEUED" {
			t.Fatal(res)
		}
	}

	if res, err := redis.String(c.Do("prepare")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("prepare not ok", res)
		}
	}

	if res, err := redis.Strings(c.Do("exec")); err != nil {
		t.Fatal(res, err)
	} else {
		if len(res) != 2 {
			t.Fatal("len err", len(res))
		}
		if res[0] != "a" {
			t.Fatal(res[0])
		}
		if res[1] != "b" {
			t.Fatal(res[1])
		}
	}
}

func TestTxDiscardOnly(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	if _, err := c.Do("discard"); err != nil {
		if err.Error() != errn.ErrDiscardNoMulti.Error() {
			t.Fatal(err)
		}
	}
}

func TestTxDiscardWatch(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	key := "a"
	if res, err := redis.String(c.Do("watch", key)); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if _, err := c.Do("discard"); err != nil {
		if err.Error() != errn.ErrDiscardNoMulti.Error() {
			t.Fatal(err)
		}
	}
}

func TestTxDiscardMulti(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	key := "a"
	if res, err := redis.String(c.Do("watch", key)); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if res, err := c.Do("discard"); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
}

func TestTxDiscardPrepare(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	key := "a"
	if res, err := redis.String(c.Do("watch", key)); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if _, err := c.Do("get", key); err != nil {
		t.Fatal(err)
	}
	if res, err := redis.String(c.Do("prepare")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if res, err := c.Do("discard"); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
}

func TestTxDiscardPrepareWatchChanged(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()

	key := "a"
	val := "e"
	if res, err := redis.String(c.Do("watch", key)); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if res, err := redis.String(c.Do("set", key, val)); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if _, err := c.Do("get", key); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do("prepare"); err != nil {
		if err.Error() != errn.ErrWatchKeyChanged.Error() {
			t.Fatal(err)
		}
	}
	if _, err := c.Do("discard"); err != nil {
		if err.Error() != errn.ErrDiscardNoMulti.Error() {
			t.Fatal(err)
		}
	}
}

func TestTxDiscard3KeyNoChange(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()
	c2 := getTestConn()
	defer c2.Close()

	otherWatchKey := "other-watch-key"
	if res, err := redis.String(c2.Do("watch", otherWatchKey)); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}

	selfUpdateKey := "self-update-key"
	key := "a"
	val := "e"
	if res, err := redis.String(c.Do("watch", key)); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if _, err := c.Do("set", key, val); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do("set", otherWatchKey, val); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do("set", selfUpdateKey, val); err != nil {
		t.Fatal(err)
	}

	if res, err := redis.String(c.Do("prepare")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if res, err := c.Do("discard"); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
}

func TestTxDiscard3KeyUnlockTimeout(t *testing.T) {
	if isSkipTestTx() {
		return
	}

	c := getTestConn()
	defer c.Close()
	c2 := getTestConn()
	defer c2.Close()

	otherWatchKey := "other-watch-key"
	if res, err := redis.String(c2.Do("watch", otherWatchKey)); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}

	selfUpdateKey := "self-update-key"
	key := "a"
	val := "e"
	if res, err := redis.String(c.Do("watch", key)); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}

	if res, err := redis.String(c.Do("multi")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	if _, err := c.Do("set", key, val); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do("set", otherWatchKey, val); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do("set", selfUpdateKey, val); err != nil {
		t.Fatal(err)
	}

	if res, err := redis.String(c.Do("prepare")); err != nil {
		t.Fatal(err)
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
	time.Sleep(5 * time.Second)
	if res, err := c.Do("discard"); err != nil {
		if err.Error() != errn.ErrDiscardNoMulti.Error() {
			t.Fatal(err)
		}
	} else {
		if res != "OK" {
			t.Fatal("res is not ok", res)
		}
	}
}
