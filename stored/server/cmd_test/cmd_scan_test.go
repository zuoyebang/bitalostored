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
	"reflect"
	"testing"

	"github.com/gomodule/redigo/redis"
)

func TestScan(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	if n, err := redis.Values(c.Do("scan", "0")); err != nil {
		t.Fatal(err)
	} else if len(n) != 2 {
		t.Fatal("scan fail")
	}

	if n, err := redis.Values(c.Do("scan", "0", "count", "10")); err != nil {
		t.Fatal(err)
	} else if len(n) != 2 {
		t.Fatal("scan fail")
	}

	if _, err := redis.Values(c.Do("scan", "0", "count", "10000")); err == nil {
		t.Fatal(err)
	} else if err.Error() != "ERR count more than 5000" {
		t.Fatal("scan fail")
	}

	if n, err := redis.Values(c.Do("scan", "0", "match", "")); err != nil {
		t.Fatal(err)
	} else if len(n) != 2 {
		t.Fatal("scan fail")
	}

	if n, err := redis.Values(c.Do("scan", "0", "type", "string")); err != nil {
		t.Fatal(err)
	} else if len(n) != 2 {
		t.Fatal("scan fail")
	}
	if n, err := redis.Values(c.Do("scan", "0", "type", "hash")); err != nil {
		t.Fatal(err)
	} else if len(n) != 2 {
		t.Fatal("scan fail")
	}
	if n, err := redis.Values(c.Do("scan", "0", "type", "set")); err != nil {
		t.Fatal(err)
	} else if len(n) != 2 {
		t.Fatal("scan fail")
	}
	if n, err := redis.Values(c.Do("scan", "0", "type", "list")); err != nil {
		t.Fatal(err)
	} else if len(n) != 2 {
		t.Fatal("scan fail")
	}
	if n, err := redis.Values(c.Do("scan", "0", "type", "zset")); err != nil {
		t.Fatal(err)
	} else if len(n) != 2 {
		t.Fatal("scan fail")
	}

	c.Do("set", "teststring1", "string1")
	if act, err := c.Do("scan", "0", "type", "string", "match", "teststring1"); err != nil {
		t.Fatal(err)
	} else {
		exp := []interface{}{[]byte("0"), []interface{}{[]byte("teststring1")}}
		if !reflect.DeepEqual(exp, act) {
			t.Fatal("scan fail")
		}
	}
	if act, err := c.Do("scan", "0", "type", "string", "match", "teststring11*"); err != nil {
		t.Fatal(err)
	} else {
		exp := []interface{}{[]byte("0"), []interface{}{}}
		if !reflect.DeepEqual(exp, act) {
			t.Fatal("scan fail")
		}
	}

	c.Do("hset", "testhash1", "h1", "v1")
	if act, err := c.Do("scan", "0", "type", "hash", "match", "testhash1"); err != nil {
		t.Fatal(err)
	} else {
		exp := []interface{}{[]byte("0"), []interface{}{[]byte("testhash1")}}
		if !reflect.DeepEqual(exp, act) {
			t.Fatal("scan fail")
		}
	}
	if act, err := c.Do("scan", "0", "type", "hash", "match", "testhash11*"); err != nil {
		t.Fatal(err)
	} else {
		exp := []interface{}{[]byte("0"), []interface{}{}}
		if !reflect.DeepEqual(exp, act) {
			t.Fatal("scan fail")
		}
	}

	c.Do("sadd", "testset1", "m1", "m2")
	if act, err := c.Do("scan", "0", "type", "set", "match", "testset1"); err != nil {
		t.Fatal(err)
	} else {
		exp := []interface{}{[]byte("0"), []interface{}{[]byte("testset1")}}
		if !reflect.DeepEqual(exp, act) {
			t.Fatal("scan fail")
		}
	}
	if act, err := c.Do("scan", "0", "type", "set", "match", "testset11*"); err != nil {
		t.Fatal(err)
	} else {
		exp := []interface{}{[]byte("0"), []interface{}{}}
		if !reflect.DeepEqual(exp, act) {
			t.Fatal("scan fail")
		}
	}

	c.Do("zadd", "testzset1", 1, "m1")
	if act, err := c.Do("scan", "0", "type", "zset", "match", "testzset1"); err != nil {
		t.Fatal(err)
	} else {
		exp := []interface{}{[]byte("0"), []interface{}{[]byte("testzset1")}}
		if !reflect.DeepEqual(exp, act) {
			t.Fatal("scan fail")
		}
	}
	if act, err := c.Do("scan", "0", "type", "zset", "match", "testzset11*"); err != nil {
		t.Fatal(err)
	} else {
		exp := []interface{}{[]byte("0"), []interface{}{}}
		if !reflect.DeepEqual(exp, act) {
			t.Fatal("scan fail")
		}
	}
}
