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
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
)

func TestDkCmds(t *testing.T) {
	closeServer, err := startServer(testDBConf, testDBPort)
	require.NoError(t, err)
	defer closeServer()

	time.Sleep(100 * time.Millisecond)

	c := getTestConnWithAddr(testDBPort)
	defer c.Close()

	// Test DKCMD operations
	var dtStr string
	var dataType int
	ts := int(tclock.GetTimestampSecond() + 10)
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		shardNum := uint32(i + 1)
		if i%2 == 0 {
			dtStr = "hash"
			dataType = int(btools.DataTypeHash)
		} else {
			dtStr = "set"
			dataType = int(btools.DataTypeSet)
		}
		ok, err := redis.String(c.Do("dk.create", key, shardNum, []byte(dtStr)))
		require.NoError(t, err)
		require.Equal(t, resp.ReplyOK, ok)

		n, err := redis.Int(c.Do("dk.incrbysize", key, 10))
		require.NoError(t, err)
		require.Equal(t, 10, n)

		size, err := redis.Int(c.Do("dk.incrbysize", key, -2))
		require.NoError(t, err)
		require.Equal(t, 8, size)

		res, err := redis.Ints(c.Do("dk.info", key))
		require.NoError(t, err)
		require.Equal(t, dataType, res[0])
		require.Equal(t, int(shardNum), res[1])
		require.Equal(t, size, res[2])
		require.Equal(t, -1, res[3])

		n, err = redis.Int(c.Do("expireat", key, ts))
		require.NoError(t, err)
		require.Equal(t, 1, n)

		res, err = redis.Ints(c.Do("dk.info", key))
		require.NoError(t, err)
		require.Equal(t, dataType, res[0])
		require.Equal(t, int(shardNum), res[1])
		require.Equal(t, size, res[2])
		if res[3] < 8 {
			t.Fatalf("ttl should be larger than 1 ttl=%d", res[3])
		}

		if i%3 == 0 {
			n, err = redis.Int(c.Do("del", key))
			require.NoError(t, err)
			require.Equal(t, 1, n)
		}

		res, err = redis.Ints(c.Do("dk.info", key))
		if err != nil {
			t.Fatal(err)
		}
		if i%3 == 0 {
			require.Equal(t, 0, res[0])
			require.Equal(t, 0, res[1])
			require.Equal(t, 0, res[2])
			require.Equal(t, -2, res[3])

			ok, err = redis.String(c.Do("dk.create", key, shardNum, []byte(dtStr)))
			require.NoError(t, err)
			require.Equal(t, resp.ReplyOK, ok)
			n, err = redis.Int(c.Do("dk.incrbysize", key, 10))
			require.NoError(t, err)
			require.Equal(t, 10, n)

			res, err = redis.Ints(c.Do("dk.info", key))
			require.Equal(t, dataType, res[0])
			require.Equal(t, int(shardNum), res[1])
			require.Equal(t, n, res[2])
			require.Equal(t, -1, res[3])
		} else {
			require.Equal(t, dataType, res[0])
			require.Equal(t, int(shardNum), res[1])
			require.Equal(t, size, res[2])
			if res[3] < 5 {
				t.Fatalf("ttl should be larger than 1 ttl=%d", res[3])
			}
		}
	}

	res, err := redis.Ints(c.Do("dk.info", []byte("nonedkkey")))
	require.NoError(t, err)
	require.Equal(t, 0, res[0])
	require.Equal(t, 0, res[1])
	require.Equal(t, 0, res[2])
	require.Equal(t, -2, res[3])

	// Test DKHash operations
	var cks, wargs, rargs, dargs, dres []interface{}
	var rres [][]byte
	kn := 10
	kkn := 10
	kknBytes := extend.FormatIntToSlice(kkn)
	del := kkn / 2
	delBytes := extend.FormatIntToSlice(del)
	cks = append(cks, []byte("hash"))
	for i := 0; i < kn; i++ {
		key := []byte(fmt.Sprintf("dkhash_%d", i))
		cks = append(cks, key)
		wargs = append(wargs, key, kknBytes)
		dargs = append(dargs, key, delBytes)
		rargs = append(rargs, key, kknBytes)
		rres = append(rres, key, kknBytes)
		dres = append(dres, key, kknBytes)
		for j := 0; j < kkn; j++ {
			f := []byte(fmt.Sprintf("dkhashfield_%d_%d", i, j))
			v := []byte(fmt.Sprintf("dkhashvalue_%d_%d", i, j))
			wargs = append(wargs, f, v)
			rargs = append(rargs, f)
			rres = append(rres, v)
			if j%2 == 0 {
				dargs = append(dargs, f)
				dres = append(dres, []byte(nil))
			} else {
				dres = append(dres, v)
			}
		}
	}

	resSet, errSet := redis.ByteSlices(c.Do("dk.hmset", wargs...))
	if errSet == nil {
		t.Fatal("expect err but got nil")
	}

	ok, err := redis.String(c.Do("dk.createshard", cks...))
	require.NoError(t, err)
	require.Equal(t, resp.ReplyOK, ok)

	resSet, errSet = redis.ByteSlices(c.Do("dk.hset", wargs...))
	require.NoError(t, errSet)
	require.Equal(t, kn*2, len(resSet))
	for i := 0; i < kn; i++ {
		key := []byte(fmt.Sprintf("dkhash_%d", i))
		require.Equal(t, key, resSet[i*2])
		require.Equal(t, kknBytes, resSet[i*2+1])

		f := []byte(fmt.Sprintf("dkhashfield_%d_%d", i, 0))
		v := []byte(fmt.Sprintf("dkhashvalue_%d_%d", i, 0))
		resGet, errGet := redis.Bytes(c.Do("dk.hget", key, f))
		require.NoError(t, errGet)
		require.Equal(t, v, resGet)
	}

	resGet, errGet := redis.ByteSlices(c.Do("dk.hmget", rargs...))
	require.NoError(t, errGet)
	require.Equal(t, kn*(2+kkn), len(resGet))
	for i := range rres {
		require.Equal(t, rres[i], resGet[i])
	}

	resDel, errDel := redis.ByteSlices(c.Do("dk.hdel", dargs...))
	require.NoError(t, errDel)
	require.Equal(t, kn*2, len(resDel))
	for i := 0; i < kn; i++ {
		key := []byte(fmt.Sprintf("dkhash_%d", i))
		require.Equal(t, key, resDel[i*2])
		require.Equal(t, delBytes, resDel[i*2+1])
	}

	resGet, errGet = redis.ByteSlices(c.Do("dk.hmget", rargs...))
	require.NoError(t, errGet)
	require.Equal(t, kn*(2+kkn), len(resGet))
	for i := range rres {
		require.Equal(t, dres[i], resGet[i])
	}

	// Test DKSet operations
	var cksSet, wargsSet, dargsSet []interface{}
	knSet := 10
	kknSet := 10
	kknBytesSet := extend.FormatIntToSlice(kknSet)
	delSet := kknSet / 2
	delBytesSet := extend.FormatIntToSlice(delSet)
	cksSet = append(cksSet, []byte("set"))
	for i := 0; i < knSet; i++ {
		key := []byte(fmt.Sprintf("dkset_%d", i))
		cksSet = append(cksSet, key)
		wargsSet = append(wargsSet, key, kknBytesSet)
		dargsSet = append(dargsSet, key, delBytesSet)
		for j := 0; j < kknSet; j++ {
			f := []byte(fmt.Sprintf("dksetfield_%d", j))
			wargsSet = append(wargsSet, f)
			if j%2 == 0 {
				dargsSet = append(dargsSet, f)
			}
		}
	}

	resSetSet, errSetSet := redis.ByteSlices(c.Do("dk.sadd", wargsSet...))
	if errSetSet == nil {
		t.Fatal("expect err but got nil")
	}

	ok, err = redis.String(c.Do("dk.createshard", cksSet...))
	require.NoError(t, err)
	require.Equal(t, resp.ReplyOK, ok)

	resSetSet, errSetSet = redis.ByteSlices(c.Do("dk.sadd", wargsSet...))
	require.NoError(t, errSetSet)
	require.Equal(t, knSet*2, len(resSetSet))
	for i := 0; i < knSet; i++ {
		key := []byte(fmt.Sprintf("dkset_%d", i))
		require.Equal(t, key, resSetSet[i*2])
		require.Equal(t, kknBytesSet, resSetSet[i*2+1])
	}

	for i := 0; i < knSet; i++ {
		key := []byte(fmt.Sprintf("dkset_%d", i))
		for j := 0; j < kknSet; j++ {
			f := []byte(fmt.Sprintf("dksetfield_%d", j))
			n, err := redis.Int(c.Do("sismember", key, f))
			require.NoError(t, err)
			require.Equal(t, 1, n)
		}
	}

	resDelSet, errDelSet := redis.ByteSlices(c.Do("dk.srem", dargsSet...))
	require.NoError(t, errDelSet)
	require.Equal(t, knSet*2, len(resSetSet))
	for i := 0; i < knSet; i++ {
		key := []byte(fmt.Sprintf("dkset_%d", i))
		require.Equal(t, key, resDelSet[i*2])
		require.Equal(t, delBytesSet, resDelSet[i*2+1])
	}

	for i := 0; i < knSet; i++ {
		key := []byte(fmt.Sprintf("dkset_%d", i))
		for j := 0; j < kknSet; j++ {
			f := []byte(fmt.Sprintf("dksetfield_%d", j))
			n, err := redis.Int(c.Do("sismember", key, f))
			require.NoError(t, err)
			if j%2 == 0 {
				require.Equal(t, 0, n)
			} else {
				require.Equal(t, 1, n)
			}
		}
	}
}
