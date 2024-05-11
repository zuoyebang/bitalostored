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

package engine

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/config"

	"github.com/zuoyebang/bitalostored/butils/hash"
)

func TestDoCheckpoint(t *testing.T) {
	config.GlobalConfig.Plugin.OpenRaft = false
	const testDir = "testdir"
	srcDir := filepath.Join(testDir, "src")
	destDir := filepath.Join(testDir, "dest")
	os.RemoveAll(testDir)
	os.MkdirAll(srcDir, 0755)
	os.MkdirAll(destDir, 0755)
	defer func() {
		os.RemoveAll(testDir)
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
		zaddArgs := btools.ScorePair{
			Member: []byte("member"),
			Score:  1,
		}
		if n, err := db.ZAdd([]byte("test-zset"), hash.Fnv32([]byte("test-zset")), zaddArgs); err != nil {
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

		if v, vCloser, err := db.HGet([]byte("test-hash"), hash.Fnv32([]byte("test-hash")), []byte("member")); err != nil {
			t.Fatal(err)
		} else if string(v) != "1" {
			t.Fatalf("expect: %s, actual: %s", "1", v)
		} else {
			vCloser()
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
	}

	fmt.Println("db1 open")
	db1, err := NewBitalos(srcDir)
	if err != nil {
		t.Fatal("init db error")
	}
	writeData(db1)
	fmt.Println("db1 open readData")
	readData(db1)
	db1.Close()

	db1, err = NewBitalos(srcDir)
	if err != nil {
		t.Fatal("init db error")
	}

	fmt.Println("db1 start checkpoint")
	snapshotDir := filepath.Join(srcDir, "snapshot")
	db1.Meta.SetUpdateIndex(123)
	if _, err := db1.DoSnapshot(snapshotDir); err != nil {
		t.Fatal(err)
	}
	defer db1.Close()

	updateIndex := strconv.FormatInt(int64(db1.Meta.GetUpdateIndex()), 10)
	copySnapshotDir := filepath.Join(snapshotDir, updateIndex)
	dstSnapshotDir := filepath.Join(destDir, updateIndex)
	fmt.Println("db1 copySnapshotDir", copySnapshotDir, dstSnapshotDir)
	cmd := exec.Command("cp", "-rf", copySnapshotDir, dstSnapshotDir)
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}

	db2, err := NewBitalos(dstSnapshotDir)
	if err != nil {
		t.Fatal("init db error")
	}
	defer db2.Close()
	readData(db2)
}
