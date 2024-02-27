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

package bitsdb

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"

	"github.com/zuoyebang/bitalostored/butils/hash"

	"github.com/stretchr/testify/require"
)

func TestHashVersionIter(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("hash_hincrby_test_cc")
	khash := hash.Fnv32(key)
	args := make([]btools.FVPair, 0, 2)
	args = append(args, btools.FVPair{
		Field: []byte("a"),
		Value: []byte("hello world 1"),
	})
	args = append(args, btools.FVPair{
		Field: []byte("b"),
		Value: []byte("hello world 2"),
	})

	if err := bdb.HashObj.HMset(key, khash, args...); err != nil {
		t.Fatal(err)
	}
	keyId := bdb.HashObj.GetCurrentKeyId()
	if _, err := bdb.StringObj.Expire(key, khash, 3); err != nil {
		t.Fatal(err)
	}

	var lowerBound [base.DataKeyHeaderLength]byte
	var upperBound [base.DataKeyUpperBoundLength]byte
	base.EncodeDataKeyLowerBound(lowerBound[:], keyId, khash)
	base.EncodeDataKeyUpperBound(upperBound[:], keyId, khash)
	iterOpts := &bitskv.IterOptions{
		KeyHash:    khash,
		UpperBound: upperBound[:],
	}
	it := bdb.HashObj.DataDb.NewIterator(iterOpts)
	defer it.Close()
	for it.Seek(lowerBound[:]); it.Valid(); it.Next() {
		field, ver, err := base.DecodeDataKey(it.RawKey())
		fmt.Println(field, ver, err)
	}

	time.Sleep(5 * time.Second)

	if err := bdb.HashObj.HMset(key, khash, args...); err != nil {
		t.Fatal(err)
	}
	if _, err := bdb.StringObj.Expire(key, khash, 3); err != nil {
		t.Fatal(err)
	}

	for it.Seek(lowerBound[:]); it.Valid(); it.Next() {
		field, ver, err := base.DecodeDataKey(it.Key())
		fmt.Println(field, ver, err)
	}

	it1 := bdb.HashObj.DataDb.NewIterator(iterOpts)
	defer it1.Close()
	for it1.Seek(lowerBound[:]); it1.Valid(); it1.Next() {
		field, ver, err := base.DecodeDataKey(it1.Key())
		fmt.Println(field, ver, err)
	}
}

func TestHashCode(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("key")
	field := []byte("field")
	keyId := bdb.HashObj.GetNextKeyId()

	khash := hash.Fnv32(key)
	ek, ekCloser := base.EncodeMetaKey(key, khash)
	defer ekCloser()
	if k, err := base.DecodeMetaKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "key" {
		t.Fatal(string(k))
	}

	mkv := &base.MetaData{}
	mkv.Reset(keyId)
	require.Equal(t, base.KeyKindDefault, mkv.Kind())

	ekf, ekfCloser := base.EncodeDataKey(keyId, khash, field)
	defer ekfCloser()
	if f, version, err := base.DecodeDataKey(ekf); err != nil {
		t.Fatal(err)
	} else if version != keyId {
		t.Fatal("version err", version, keyId)
	} else if string(f) != "field" {
		t.Fatal("field err", string(f))
	}
}

func TestHsetEmptyValueAndCheckLen(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	// case1: hset key field nil, hlen=1; hset key filed value, hlen=1
	// case2: hset key field value, hlen=1; hset key filed nil, hlen=1
	key := []byte("hash_empty_key")
	khash := hash.Fnv32(key)
	field1 := []byte("f1")
	field2 := []byte("f2")
	var emptyValue []byte
	value := []byte("value")

	hsetTwice := func(key []byte, field []byte, first []byte, second []byte) {
		khash2 := hash.Fnv32(key)
		if n, err := bdb.HashObj.HSet(key, khash2, field, first); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.HashObj.HLen(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		v, vCloser, err := bdb.HashObj.HGet(key, khash, field)
		if err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(v, first) {
			t.Fatalf("value not equal. actual:%s, expect:%s", string(v), string(first))
		}
		vCloser()

		if n, err := bdb.HashObj.HSet(key, khash2, field, second); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
		if n, err := bdb.HashObj.HLen(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		v, vCloser, err = bdb.HashObj.HGet(key, khash, field)
		if err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(v, second) {
			t.Fatalf("value not equal. actual:%s, expect:%s", string(v), string(first))
		}
		vCloser()
	}
	hsetTwice(key, field1, emptyValue, value)
	bdb.HashObj.Del(khash, key)
	hsetTwice(key, field2, value, emptyValue)
	bdb.HashObj.Del(khash, key)
}

func TestHmsetEmptyValueAndCheckLen(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("hmset_empty_key")
	khash := hash.Fnv32(key)
	field1 := []byte("f1")
	field2 := []byte("f2")
	emptyValue := []byte{}
	value := []byte("value")
	hmsetFunc := func(key []byte, field1 []byte, field2 []byte, first []byte, second []byte) {
		pair1 := btools.FVPair{Field: field1, Value: first}
		pair2 := btools.FVPair{Field: field2, Value: second}
		if err := bdb.HashObj.HMset(key, khash, pair1, pair2); err != nil {
			t.Fatal(err)
		}

		if n, err := bdb.HashObj.HLen(key, khash); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}
	}

	hmsetFunc(key, field1, field2, emptyValue, value)
	hmsetFunc(key, field1, field2, value, emptyValue)
	bdb.HashObj.Del(khash, key)
}

func TestHashGDel(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		khash := hash.Fnv32(key)
		if n, err := bdb.HashObj.HSet(key, khash, []byte("a"), []byte("hello world 1")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.HashObj.Del(khash, key); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.HashObj.HLen(key, hash.Fnv32(key)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}
}

func TestHashHSet_HGet_HDEL_HLEN(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	checkKeyKind := func(k []byte, h uint32, kind uint8) {
		mk, mkCloser := base.EncodeMetaKey(k, h)
		mkv, err := bdb.HashObj.GetMetaData(mk)
		mkCloser()
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, kind, mkv.Kind())
	}

	key1 := []byte("testhash1")
	khash1 := hash.Fnv32(key1)
	if n, err := bdb.HashObj.HSet(key1, khash1, []byte("h1"), []byte("v1")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}
	data1, vCloser1, err1 := bdb.HashObj.HGet(key1, khash1, []byte("h1"))
	if err1 != nil {
		t.Fatal(err1)
	}
	if string(data1) != "v1" {
		t.Fatal(string(data1))
	}
	vCloser1()
	if n, err := bdb.HashObj.HSet(key1, khash1, []byte("h1"), []byte("v1")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
	if n, err := bdb.HashObj.HSet(key1, khash1, []byte("h1"), []byte("v11")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
	checkKeyKind(key1, khash1, base.KeyKindDefault)

	for i := 0; i < 10; i++ {
		key := []byte("hash_hset_hget_hdel_test")
		khash := hash.Fnv32(key)
		if n, err := bdb.HashObj.HSet(key, khash, []byte("a"), []byte("hello world 1")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		data, vCloser, err := bdb.HashObj.HGet(key, khash, []byte("a"))
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "hello world 1" {
			t.Fatal(string(data))
		}
		vCloser()

		if n, err := bdb.HashObj.HSet(key, khash, []byte("a"), []byte("hello world 1")); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
		if n, err := bdb.HashObj.HSet(key, khash, []byte("a"), []byte("hello world 11")); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if n, err := bdb.HashObj.HSet(key, khash, []byte("b"), []byte("hello world 2")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		checkKeyKind(key, khash, base.KeyKindDefault)

		if n, err := bdb.HashObj.HLen(key, hash.Fnv32(key)); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatal(n)
		}

		if n, err := bdb.HashObj.HDel(key, khash, []byte("a")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}

		if n, err := bdb.HashObj.HDel(key, khash, []byte("a")); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}

		if n, err := bdb.HashObj.HLen(key, hash.Fnv32(key)); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.HashObj.Del(khash, key); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := bdb.HashObj.HLen(key, hash.Fnv32(key)); err != nil {
			t.Fatal(err)
		} else if n != 0 {
			t.Fatal(n)
		}
	}
}

func TestHashHMSet_HmGet_HGETALL_HKEYS_HVAL(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("hash_hmset_hmget_hkeys_hgetall")
	khash := hash.Fnv32(key)
	args := make([]btools.FVPair, 0, 2)
	args = append(args, btools.FVPair{
		Field: []byte("a"),
		Value: []byte("hello world 1"),
	})
	args = append(args, btools.FVPair{
		Field: []byte("b"),
		Value: []byte("hello world 2"),
	})

	if err := bdb.HashObj.HMset(key, khash, args...); err != nil {
		t.Fatal(err)
	}

	if err := bdb.HashObj.HMset(key, khash, args...); err != nil {
		t.Fatal(err)
	}

	ay, vClosers, err := bdb.HashObj.HMget(key, hash.Fnv32(key), []byte("a"), []byte("b"))
	if err != nil {
		t.Fatal(err)
	} else {
		if v1 := ay[0]; string(v1) != "hello world 1" {
			t.Fatal(string(v1))
		}

		if v2 := ay[1]; string(v2) != "hello world 2" {
			t.Fatal(string(v2))
		}
	}
	if len(vClosers) > 0 {
		for _, vCloser := range vClosers {
			vCloser()
		}
	}

	ay, vClosers, err = bdb.HashObj.HMget(key, hash.Fnv32(key), []byte("a"), []byte("c"))
	if err != nil {
		t.Fatal(err)
	} else {
		if v1 := ay[0]; string(v1) != "hello world 1" {
			t.Fatal(string(v1))
		}

		if v2 := ay[1]; string(v2) != "" {
			t.Fatal(string(v2))
		}
	}
	if len(vClosers) > 0 {
		for _, vCloser := range vClosers {
			vCloser()
		}
	}

	ay, vClosers, err = bdb.HashObj.HMget(key, hash.Fnv32(key), []byte("c"), []byte("a"))
	if err != nil {
		t.Fatal(err)
	} else {
		if v1 := ay[0]; string(v1) != "" {
			t.Fatal(string(v1))
		}

		if v2 := ay[1]; string(v2) != "hello world 1" {
			t.Fatal(string(v2))
		}
	}
	if len(vClosers) > 0 {
		for _, vCloser := range vClosers {
			vCloser()
		}
	}

	resHgetall, resHgetallClosers, err := bdb.HashObj.HGetAll(key, khash)
	defer func() {
		if len(resHgetallClosers) > 0 {
			for _, resHgetallCloser := range resHgetallClosers {
				resHgetallCloser()
			}
		}
	}()
	if err != nil {
		t.Fatal(err)
	} else {
		if len(resHgetall) != 2 {
			t.Fatal("hgetall len err")
		}
		if string(resHgetall[0].Value) != "hello world 1" {
			t.Fatal(string(resHgetall[0].Value))
		}
		if string(resHgetall[1].Value) != "hello world 2" {
			t.Fatal(string(resHgetall[1].Value))
		}
	}

	resHkeys, resHkeysClosers, err := bdb.HashObj.HKeys(key, khash)
	defer func() {
		if len(resHkeysClosers) > 0 {
			for _, resHkeysCloser := range resHkeysClosers {
				resHkeysCloser()
			}
		}
	}()
	if err != nil {
		t.Fatal(err)
	} else {
		if len(resHkeys) != 2 {
			t.Fatal("hgetall len err")
		}
		if string(resHkeys[0]) != "a" {
			t.Fatal(string(resHkeys[0]))
		}
		if string(resHkeys[1]) != "b" {
			t.Fatal(string(resHkeys[1]))
		}
	}

	resHValues, resHValuesClosers, err := bdb.HashObj.HValues(key, khash)
	defer func() {
		if len(resHValuesClosers) > 0 {
			for _, resHValuesCloser := range resHValuesClosers {
				resHValuesCloser()
			}
		}
	}()
	if err != nil {
		t.Fatal(err)
	} else {
		if len(resHValues) != 2 {
			t.Fatal("hgetall len err")
		}
		if string(resHValues[0]) != "hello world 1" {
			t.Fatal(string(resHValues[0]))
		}
		if string(resHValues[1]) != "hello world 2" {
			t.Fatal(string(resHValues[1]))
		}
	}
}

func TestHashTTL(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("hash_ttl_test")
	khash := hash.Fnv32(key)
	field := []byte("field")
	val := []byte("aa")
	if n, err := bdb.HashObj.HSet(key, khash, field, val); err != nil {
		t.Fatal(err)
	} else {
		t.Log("hset=", n)
	}

	if n, err := bdb.StringObj.Expire(key, khash, 2); err != nil {
		t.Fatal(err)
	} else {
		t.Log("Expire=", n)
	}

	if ttl, err := bdb.StringObj.TTL(key, khash); err != nil {
		t.Fatal(err)
	} else {
		t.Log("ttl=", ttl)
	}

	data, vCloser, err := bdb.HashObj.HGet(key, khash, field)
	if err != nil {
		t.Fatal(err)
	} else if string(data) != "aa" {
		t.Fatal(string(data))
	}
	vCloser()

	time.Sleep(2 * time.Second)

	if n, err := bdb.StringObj.Exists(key, khash); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
}

func TestHashExists(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("hash_exists_test_a")
	khash := hash.Fnv32(key)

	v, err := bdb.StringObj.Exists(key, khash)
	if err != nil {
		t.Fatal(err.Error())
	}
	if v != 0 {
		t.Fatal("invalid value ", v)
	}

	if _, err := bdb.HashObj.HSet(key, khash, []byte("hello"), []byte("world")); err != nil {
		t.Fatal(err.Error())
	}

	v, err = bdb.StringObj.Exists(key, khash)
	if err != nil {
		t.Fatal(err.Error())
	}
	if v != 1 {
		t.Fatal("invalid value ", v)
	}
}

func TestHashHincrBy(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("hash_hincrby_test_cc")
	khash := hash.Fnv32(key)
	field := []byte("hash_hincrby")

	if _, err := bdb.HashObj.HIncrBy(key, khash, field, 1); err != nil {
		t.Fatal(err.Error())
	}
	if _, err := bdb.HashObj.HIncrBy(key, khash, field, 1); err != nil {
		t.Fatal(err.Error())
	}
	data, vCloser, err := bdb.HashObj.HGet(key, khash, field)
	if err != nil {
		t.Fatal(err)
	} else if string(data) != "2" {
		t.Fatal(string(data))
	}
	vCloser()
}

func TestDBHScan(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("scan_h_key")
	khash := hash.Fnv32(key)
	value := []byte("hello world")
	bdb.HashObj.HSet(key, khash, []byte("1"), value)
	bdb.HashObj.HSet(key, khash, []byte("222"), value)
	bdb.HashObj.HSet(key, khash, []byte("19"), value)
	bdb.HashObj.HSet(key, khash, []byte("1234"), value)
	bdb.HashObj.HSet(key, khash, []byte("322"), value)
	bdb.HashObj.HSet(key, khash, []byte("422"), value)
	bdb.HashObj.HSet(key, khash, []byte("522"), value)
	bdb.HashObj.HSet(key, khash, []byte("622"), value)
	bdb.HashObj.HSet(key, khash, []byte("722"), value)
	bdb.HashObj.HSet(key, khash, []byte("822"), value)

	_, v, err := bdb.HashObj.HScan(key, khash, nil, 100, "**")
	if err != nil {
		t.Fatal(err)
	} else if len(v) != 10 {
		t.Fatal("invalid count", len(v))
	}

	cursor := []byte("222")
	cursor, v, err = bdb.HashObj.HScan(key, khash, cursor, 2, "**")

	t.Log(string(cursor))
	for _, d := range v {
		t.Log(string(d.Field), "=>", string(d.Value))
	}

	if err != nil {
		t.Fatal(err)
	} else if len(v) != 2 {
		t.Fatal("invalid count", len(v))
	} else if string(cursor) != "422" {
		t.Fatal(string(cursor))
	}

	bdb.HashObj.Del(khash, key)
}

func TestHashHMSet(t *testing.T) {
	bdb := testNewBitsDB()
	defer closeDb(bdb)

	key := []byte("hash_hmset_1")
	khash := hash.Fnv32(key)
	args := make([]btools.FVPair, 0, 3)
	args = append(args, btools.FVPair{
		Field: []byte("online"),
		Value: []byte("0"),
	})
	args = append(args, btools.FVPair{
		Field: []byte("idconline"),
		Value: []byte("2:0"),
	})
	args = append(args, btools.FVPair{
		Field: []byte("stream"),
		Value: []byte("0"),
	})

	if err := bdb.HashObj.HMset(key, khash, args...); err != nil {
		t.Fatal(err)
	}

	args = make([]btools.FVPair, 0, 3)
	args = append(args, btools.FVPair{
		Field: []byte("online"),
		Value: []byte("1"),
	})
	args = append(args, btools.FVPair{
		Field: []byte("idconline"),
		Value: []byte("2:0"),
	})
	args = append(args, btools.FVPair{
		Field: []byte("stream"),
		Value: []byte("0"),
	})
	if err := bdb.HashObj.HMset(key, khash, args...); err != nil {
		t.Fatal(err)
	}

	resHgetall, resHgetallClosers, err := bdb.HashObj.HGetAll(key, khash)
	if err != nil {
		t.Fatal(err)
	} else if len(resHgetall) != 3 {
		t.Fatal("resHgetall len err", len(resHgetall))
	}
	if string(resHgetall[0].Field) != "idconline" {
		t.Fatal("0 Field err")
	} else if string(resHgetall[0].Value) != "2:0" {
		t.Fatal("0 Value err")
	}
	if string(resHgetall[1].Field) != "online" {
		t.Fatal("1 Field err")
	} else if string(resHgetall[1].Value) != "1" {
		t.Fatal("1 Value err")
	}
	if string(resHgetall[2].Field) != "stream" {
		t.Fatal("2 Field err")
	} else if string(resHgetall[2].Value) != "0" {
		t.Fatal("2 Value err")
	}
	if len(resHgetallClosers) > 0 {
		for _, resHgetallCloser := range resHgetallClosers {
			resHgetallCloser()
		}
	}
}
