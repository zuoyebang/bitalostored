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
	"reflect"
	"testing"

	"github.com/gomodule/redigo/redis"
)

func TestGeoAdd(t *testing.T) {
	c := getTestConn()
	defer c.Close()
	c.Do("del", "Sicily")
	if n, err := redis.Int64(c.Do("geoadd", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")); err != nil {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatal("geoadd fail")
	}

	if _, err := redis.String(c.Do("geoadd", "broken", "-190.0", "10.0", "hi")); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := redis.String(c.Do("geoadd", "broken", "190.0", "10.0", "hi")); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := redis.String(c.Do("geoadd", "broken", "10.0", "-86.0", "hi")); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := redis.String(c.Do("geoadd", "broken", "10.0", "86.0", "hi")); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := redis.String(c.Do("geoadd", "broken", "notafloat", "10.0", "hi")); err == nil {
		t.Fatalf("invalid err %v", err)
	}

	if _, err := redis.String(c.Do("geoadd", "broken", "10.0", "notafloat", "hi")); err == nil {
		t.Fatalf("invalid err %v", err)
	}

}

func TestGeoPos(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	c.Do("del", "Sicily")
	c.Do("geoadd", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")

	for i := 0; i < readNum; i++ {
		if act, err := c.Do("geopos", "Sicily", "Palermo", "Catania"); err != nil {
			t.Fatal("geodist fail", err)
		} else {
			t.Log(act)
			exp := []interface{}{[]interface{}{[]byte("13.36138933897018433"), []byte("38.11555639549629859")}, []interface{}{[]byte("15.08726745843887329"), []byte("37.50266842333162032")}}
			if !reflect.DeepEqual(exp, act) {
				t.Fatal("geopos fail")
			}
		}

		if act, err := c.Do("geopos", "Sicily", "Palermo", "Catani"); err != nil {
			t.Fatal("geopos fail")
		} else {
			exp := []interface{}{[]interface{}{[]byte("13.36138933897018433"), []byte("38.11555639549629859")}, nil}
			if !reflect.DeepEqual(exp, act) {
				t.Fatal("geopos fail")
			}
		}

		if act, err := c.Do("geopos", "Sicily", "Palermo"); err != nil {
			t.Fatal("geopos fail")
		} else {
			exp := []interface{}{[]interface{}{[]byte("13.36138933897018433"), []byte("38.11555639549629859")}}
			if !reflect.DeepEqual(exp, act) {
				t.Fatal("geopos fail")
			}
		}

		if arr, err := redis.Values(c.Do("geopos", "Sicily", "Corleone")); err != nil || len(arr) != 1 {
			t.Fatal("geopos fail")
		} else if !(arr[0] == nil) {
			t.Fatal("geopos fail")
		}
	}
}

func TestGeoHash(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	c.Do("del", "Sicily")
	c.Do("geoadd", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")

	for i := 0; i < readNum; i++ {
		if act, err := c.Do("geohash", "Sicily", "Palermo", "Catania"); err != nil {
			t.Fatal("geodist fail", err)
		} else {
			exp := []interface{}{[]byte("sqc8b49rny0"), []byte("sqdtr74hyu0")}
			if !reflect.DeepEqual(exp, act) {
				t.Fatal("geohash fail", act)
			}
		}

		if act, err := c.Do("geohash", "Sicily", "Palerm", "Catani"); err != nil {
			t.Fatal("geodist fail", err)
		} else {
			exp := []interface{}{nil, nil}
			if !reflect.DeepEqual(exp, act) {
				t.Fatal("geohash fail")
			}
		}
	}
}

func TestGeoDist(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	c.Do("del", "Sicily")
	c.Do("geoadd", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	for i := 0; i < readNum; i++ {
		if dist, err := redis.String(c.Do("geodist", "Sicily", "Palermo", "Catania")); err != nil {
			t.Fatal("geodist fail", err)
		} else if dist != "166274.1516" {
			t.Fatal("geodist fail")
		}

		if act, err := c.Do("geodist", "Sicily", "Palermo", "Catani"); err != nil {
			t.Fatal("geodist fail")
		} else {
			var exp interface{}
			if !reflect.DeepEqual(exp, act) {
				t.Fatal("geohash fail")
			}
		}

		if dist, err := redis.String(c.Do("geodist", "Sicily", "Palermo", "Catania", "km")); err != nil {
			t.Fatal("geodist fail")
		} else if dist != "166.2742" {
			t.Fatal("geodist fail")
		}

		if _, err := redis.String(c.Do("geodist", "nosuch", "nosuch", "nosuch")); err == nil {
			t.Fatalf("invalid err %v", err)
		}

		if _, err := redis.String(c.Do("geodist", "Sicily", "Palermo", "nosuch")); err == nil {
			t.Fatalf("invalid err %v", err)
		}

		if _, err := redis.String(c.Do("geodist", "Sicily", "nosuch", "Catania")); err == nil {
			t.Fatalf("invalid err %v", err)
		}

		if _, err := redis.String(c.Do("geodist")); err == nil {
			t.Fatalf("invalid err %v", err)
		}

		if _, err := redis.String(c.Do("geodist", "Sicily")); err == nil {
			t.Fatalf("invalid err %v", err)
		}

		if _, err := redis.String(c.Do("geodist", "Sicily", "Palermo")); err == nil {
			t.Fatalf("invalid err %v", err)
		}

		if _, err := redis.String(c.Do("geodist", "Sicily", "Palermo", "Catania", "miles")); err == nil {
			t.Fatalf("invalid err %v", err)
		}
	}
}

func TestGeoRadius(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	c.Do("del", "Sicily")
	c.Do("del", "StoreKey")
	c.Do("geoadd", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")

	for i := 0; i < readNum; i++ {
		if act, err := c.Do("georadius", "Sicily", "15", "37", "200", "km", "WITHDIST", "WITHCOORD", "WITHHASH", "DESC"); err != nil {
			t.Fatal("georadius fail")
		} else {
			exp := []interface{}{[]interface{}{[]byte("Palermo"), []byte("190.4424"), int64(3479099956230698), []interface{}{[]byte("13.36138933897018433"), []byte("38.11555639549629859")}}, []interface{}{[]byte("Catania"), []byte("56.4413"), int64(3479447370796909), []interface{}{[]byte("15.08726745843887329"), []byte("37.50266842333162032")}}}
			if !reflect.DeepEqual(exp, act) {
				t.Fatal("georadius fail")
			}
		}

		if act, err := c.Do("georadius", "Sicily", "15", "37", "200", "km", "WITHDIST", "WITHCOORD", "WITHHASH"); err != nil {
			t.Fatal("georadius fail")
		} else {
			exp := []interface{}{[]interface{}{[]byte("Palermo"), []byte("190.4424"), int64(3479099956230698), []interface{}{[]byte("13.36138933897018433"), []byte("38.11555639549629859")}}, []interface{}{[]byte("Catania"), []byte("56.4413"), int64(3479447370796909), []interface{}{[]byte("15.08726745843887329"), []byte("37.50266842333162032")}}}
			if !reflect.DeepEqual(exp, act) {
				t.Fatal("georadius fail")
			}
		}

		if act, err := c.Do("georadius", "Sicily", "15", "37", "2000000", "ft", "WITHDIST", "WITHCOORD", "WITHHASH", "DESC"); err != nil {
			t.Fatal("georadius fail")
		} else {
			exp := []interface{}{[]interface{}{[]byte("Palermo"), []byte("624811.1215"), int64(3479099956230698), []interface{}{[]byte("13.36138933897018433"), []byte("38.11555639549629859")}}, []interface{}{[]byte("Catania"), []byte("185174.7305"), int64(3479447370796909), []interface{}{[]byte("15.08726745843887329"), []byte("37.50266842333162032")}}}
			if !reflect.DeepEqual(exp, act) {
				t.Fatal("georadius fail")
			}
		}

		if act, err := c.Do("georadius", "Sicily", "15", "37", "200", "km", "WITHCOORD", "DESC"); err != nil {
			t.Fatal("georadius fail")
		} else {
			exp := []interface{}{[]interface{}{[]byte("Palermo"), []interface{}{[]byte("13.36138933897018433"), []byte("38.11555639549629859")}}, []interface{}{[]byte("Catania"), []interface{}{[]byte("15.08726745843887329"), []byte("37.50266842333162032")}}}
			if !reflect.DeepEqual(exp, act) {
				t.Fatal("georadius fail")
			}
		}

		if act, err := c.Do("georadius", "Sicily", "15", "37", "200", "km", "WITHDIST", "DESC"); err != nil {
			t.Fatal("georadius fail")
		} else {
			exp := []interface{}{[]interface{}{[]byte("Palermo"), []byte("190.4424")}, []interface{}{[]byte("Catania"), []byte("56.4413")}}
			if !reflect.DeepEqual(exp, act) {
				t.Fatal("georadius fail")
			}
		}

		if act, err := c.Do("georadius", "Sicily", "15", "37", "200", "km", "WITHDIST", "WITHCOORD", "ASC"); err != nil {
			t.Fatal("georadius fail")
		} else {
			exp := []interface{}{[]interface{}{[]byte("Catania"), []byte("56.4413"), []interface{}{[]byte("15.08726745843887329"), []byte("37.50266842333162032")}}, []interface{}{[]byte("Palermo"), []byte("190.4424"), []interface{}{[]byte("13.36138933897018433"), []byte("38.11555639549629859")}}}
			if !reflect.DeepEqual(exp, act) {
				t.Fatal("georadius fail")
			}
		}

		if act, err := c.Do("georadius", "Sicil", "15", "37", "200", "km", "WITHDIST", "WITHCOORD", "ASC"); err != nil {
			t.Fatal("georadius fail")
		} else {
			exp := []interface{}{}
			if !reflect.DeepEqual(exp, act) {
				t.Fatal("georadius fail")
			}
		}
	}
}

func TestGeobymember(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	c.Do("del", "Sicily")
	c.Do("del", "StoreKey")
	c.Do("geoadd", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")

	for i := 0; i < readNum; i++ {
		if act, err := c.Do("georadiusbymember", "Sicily", "Palermo", "200", "km", "WITHDIST", "WITHCOORD", "WITHHASH", "ASC"); err != nil {
			t.Fatal("georadiusbymember fail")
		} else {
			exp := []interface{}{[]interface{}{[]byte("Palermo"), []byte("0.0000"), int64(3479099956230698), []interface{}{[]byte("13.36138933897018433"), []byte("38.11555639549629859")}}, []interface{}{[]byte("Catania"), []byte("166.2742"), int64(3479447370796909), []interface{}{[]byte("15.08726745843887329"), []byte("37.50266842333162032")}}}
			if !reflect.DeepEqual(exp, act) {
				t.Fatal("georadiusbymember fail")
			}
		}

		if act, err := c.Do("georadiusbymember", "Sicily", "Palermo", "200", "km", "WITHCOORD"); err != nil {
			t.Fatal("georadiusbymember fail")
		} else {
			exp := []interface{}{[]interface{}{[]byte("Palermo"), []interface{}{[]byte("13.36138933897018433"), []byte("38.11555639549629859")}}, []interface{}{[]byte("Catania"), []interface{}{[]byte("15.08726745843887329"), []byte("37.50266842333162032")}}}
			if !reflect.DeepEqual(exp, act) {
				t.Fatal("georadiusbymember fail")
			}
		}

		if act, err := c.Do("georadiusbymember", "Sicily", "Palermo", "200", "km", "WITHDIST", "ASC"); err != nil {
			t.Fatal("georadiusbymember fail")
		} else {
			exp := []interface{}{[]interface{}{[]byte("Palermo"), []byte("0.0000")}, []interface{}{[]byte("Catania"), []byte("166.2742")}}
			if !reflect.DeepEqual(exp, act) {
				t.Fatal("georadiusbymember fail")
			}
		}

		if act, err := c.Do("georadiusbymember", "Sicily", "Palermo", "200", "km", "ASC", "WITHDIST", "COUNT", "1"); err != nil {
			t.Fatal("georadiusbymember fail")
		} else {
			exp := []interface{}{[]interface{}{[]byte("Palermo"), []byte("0.0000")}}
			if !reflect.DeepEqual(exp, act) {
				t.Fatal("georadiusbymember fail")
			}
		}

		if _, err := c.Do("georadiusbymember", "Sicily", "Palerm", "200", "km"); err == nil {
			t.Fatal("georadiusbymember fail")
		} else if err.Error() != "ERR could not decode requested zset member" {
			t.Fatal("georadiusbymember fail")
		}
	}
}
