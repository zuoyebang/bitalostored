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

package engine

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/log"

	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/stretchr/testify/require"
)

func TestDoCheckpoint(t *testing.T) {
	config.GlobalConfig.Plugin.OpenRaft = false
	os.RemoveAll(testDBPath)
	srcDir := filepath.Join(testDBPath, "src")
	destDir := filepath.Join(testDBPath, "dest")
	os.MkdirAll(srcDir, 0755)
	os.MkdirAll(destDir, 0755)
	log.NewLogger(&log.Options{
		LogPath: testDBPath + "/log",
	})
	defer func() {
		os.RemoveAll(testDBPath)
		config.GlobalConfig.Plugin.OpenRaft = true
	}()

	writeData := func(db *Bitalos) {
		if err := db.Set([]byte("test-string"), hash.Fnv32([]byte("test-string")), []byte("1")); err != nil {
			t.Fatal(err)
		}
		if n, err := db.HSet([]byte("test-hash"), hash.Fnv32([]byte("test-hash")), []byte("member"), []byte("1")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := db.SAdd([]byte("test-set"), hash.Fnv32([]byte("test-set")), []byte("member")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := db.LPush([]byte("test-list"), hash.Fnv32([]byte("test-list")), []byte("member")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
		if n, err := db.ZAdd([]byte("test-zset"), hash.Fnv32([]byte("test-zset")), getFloatByte(1), []byte("member")); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatal(n)
		}
	}

	readData := func(db *Bitalos) {
		if v, closer, err := db.Get([]byte("test-string"), hash.Fnv32([]byte("test-string"))); err != nil {
			t.Fatal(err)
		} else if string(v) != "1" {
			t.Fatalf("get error. Expect: %s, actual: %s %d", "1", v, len(v))
		} else {
			if closer != nil {
				closer()
			}
		}

		if v, err := db.SIsMember([]byte("test-set"), hash.Fnv32([]byte("test-set")), []byte("member")); err != nil {
			t.Fatal(err)
		} else if v != 1 {
			t.Fatal("smember not exist")
		}

		if n, err := db.LLen([]byte("test-list"), hash.Fnv32([]byte("test-list"))); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Fatalf("expect: 1, actual: %d", n)
		}

		if v, err := db.ZScore([]byte("test-zset"), hash.Fnv32([]byte("test-zset")), []byte("member")); err != nil {
			t.Fatal(err)
		} else if int(v) != 1 {
			t.Fatalf("expect: %d, actual: %d", 1, int(v))
		}

		if v, vCloser, err := db.HGet([]byte("test-hash"), hash.Fnv32([]byte("test-hash")), []byte("member")); err != nil {
			t.Fatal(err)
		} else if string(v) != "1" {
			t.Fatalf("expect: %s, actual: %s", "1", v)
		} else {
			vCloser()
		}
	}

	cfg := testGetDefaultConfig()
	db1 := testOpenBitsDb(false, srcDir, cfg)
	writeData(db1)
	readData(db1)

	snapshotDir := filepath.Join(srcDir, "snapshot")
	clusterId := uint64(1)
	_, ssCloser, err1 := db1.DoSnapshot(snapshotDir, nil, clusterId)
	require.NoError(t, err1)

	// updateIndex := strconv.FormatInt(int64(db1.GetUpdateIndex()), 10)
	copySnapshotDir := filepath.Join(snapshotDir, strconv.Itoa(int(clusterId)))
	dstSnapshotDir := filepath.Join(testDBPath, "dest")
	require.NoError(t, os.MkdirAll(dstSnapshotDir, 0755))
	cmd := exec.Command("cp", "-rf", copySnapshotDir, dstSnapshotDir)

	require.NoError(t, cmd.Run())

	db2 := testOpenBitsDb(false, copySnapshotDir, cfg)
	readData(db2)
	db2.Close()

	ssCloser()
	db1.Close()
}
