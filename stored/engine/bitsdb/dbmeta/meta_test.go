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

package dbmeta

import (
	"os"
	"sync"
	"testing"

	"github.com/zuoyebang/bitalostored/stored/internal/config"

	"github.com/stretchr/testify/require"
)

const testDir = "./test"

func TestMeta(t *testing.T) {
	if err := os.MkdirAll(testDir, 0755); err != nil {
		panic(err)
	}
	defer os.RemoveAll(testDir)
	index, e := OpenMeta(testDir)
	if e != nil {
		t.Error(e)
	}

	index.GetSnapshotIndex()
	index.SetSnapshotIndex(10)
	index.SetSnapshotIndex(10)
	index.SetSnapshotIndex(20)
	index.SetSnapshotIndex(30)
	index.SetSnapshotIndex(40)
	index.SetSnapshotIndex(60)

	t.Log("order: ", index.GetSnapshotOrder())
	t.Log("index: ", index.GetSnapshotIndex())
	t.Log("stamp: ", index.GetSnapshotStamp())

	index.ClearSnapshot()

	t.Log("order: ", index.GetSnapshotOrder())
	t.Log("index: ", index.GetSnapshotIndex())
	t.Log("stamp: ", index.GetSnapshotStamp())
}

func TestMeta_Bitalosdb_CompressType_0(t *testing.T) {
	if err := os.MkdirAll(testDir, 0755); err != nil {
		panic(err)
	}
	defer os.RemoveAll(testDir)
	index, e := OpenMeta(testDir)
	require.NoError(t, e)

	isSet, cType := index.GetBitalosdbCompressTypeCfg()
	require.Equal(t, false, isSet)
	require.Equal(t, uint16(0), cType)

	index.SetBitalosdbCompressTypeCfg(0)

	isSet, cType = index.GetBitalosdbCompressTypeCfg()
	require.Equal(t, true, isSet)
	require.Equal(t, uint16(0), cType)

	index.Close()

	index, e = OpenMeta(testDir)
	require.NoError(t, e)

	isSet, cType = index.GetBitalosdbCompressTypeCfg()
	require.Equal(t, true, isSet)
	require.Equal(t, uint16(0), cType)
}

func TestMeta_Bitalosdb_CompressType_1(t *testing.T) {
	if err := os.MkdirAll(testDir, 0755); err != nil {
		panic(err)
	}
	defer os.RemoveAll(testDir)

	index, e := OpenMeta(testDir)
	require.NoError(t, e)

	isSet, cType := index.GetBitalosdbCompressTypeCfg()
	require.Equal(t, false, isSet)
	require.Equal(t, uint16(0), cType)

	index.SetBitalosdbCompressTypeCfg(1)

	isSet, cType = index.GetBitalosdbCompressTypeCfg()
	require.Equal(t, true, isSet)
	require.Equal(t, uint16(1), cType)

	index.Close()

	index, e = OpenMeta(testDir)
	require.NoError(t, e)

	isSet, cType = index.GetBitalosdbCompressTypeCfg()
	require.Equal(t, true, isSet)
	require.Equal(t, uint16(1), cType)
}

func TestMeta_Bitalosdb_DatabaseType_0(t *testing.T) {
	if err := os.MkdirAll(testDir, 0755); err != nil {
		panic(err)
	}
	defer os.RemoveAll(testDir)
	index, e := OpenMeta(testDir)
	require.NoError(t, e)

	isSet, cType := index.GetBitalosdbDatabaseTypeCfg()
	require.Equal(t, false, isSet)
	require.Equal(t, uint16(0), cType)

	index.SetBitalosdbDatabaseTypeCfg(0)

	isSet, cType = index.GetBitalosdbDatabaseTypeCfg()
	require.Equal(t, true, isSet)
	require.Equal(t, uint16(0), cType)

	index.Close()

	index, e = OpenMeta(testDir)
	require.NoError(t, e)

	isSet, cType = index.GetBitalosdbDatabaseTypeCfg()
	require.Equal(t, true, isSet)
	require.Equal(t, uint16(0), cType)
}

func TestMeta_Bitalosdb_DatabaseType_1(t *testing.T) {
	if err := os.MkdirAll(testDir, 0755); err != nil {
		panic(err)
	}
	defer os.RemoveAll(testDir)
	index, e := OpenMeta(testDir)
	require.NoError(t, e)

	isSet, cType := index.GetBitalosdbDatabaseTypeCfg()
	require.Equal(t, false, isSet)
	require.Equal(t, uint16(0), cType)

	index.SetBitalosdbDatabaseTypeCfg(1)

	isSet, cType = index.GetBitalosdbDatabaseTypeCfg()
	require.Equal(t, true, isSet)
	require.Equal(t, uint16(1), cType)

	index.Close()

	index, e = OpenMeta(testDir)
	require.NoError(t, e)

	isSet, cType = index.GetBitalosdbDatabaseTypeCfg()
	require.Equal(t, true, isSet)
	require.Equal(t, uint16(1), cType)
}

func TestMeta_KeyId(t *testing.T) {
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)
	meta, err := OpenMeta(testDir)
	require.NoError(t, err)

	require.Equal(t, uint64(RestartFieldKeyUniqIdGap), meta.GetCurrentKeyUniqId())
	require.Equal(t, uint64(RestartFieldKeyUniqIdGap), meta.GetDiskKeyUniqId())

	ids := make([]uint64, 100000, 100000)
	idsmap := make(map[uint64]struct{}, 100000)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				kid := meta.GetNextKeyUniqId()
				ids[index*1000+j] = kid
			}
		}(i)
	}
	wg.Wait()

	for _, v := range ids {
		if _, ok := idsmap[v]; ok {
			t.Fatal("repeat keyId", v)
		} else {
			idsmap[v] = struct{}{}
		}
	}

	require.Equal(t, 100000, len(idsmap))
	require.Equal(t, uint64(100000+RestartFieldKeyUniqIdGap), meta.GetCurrentKeyUniqId())
	require.Equal(t, uint64(110000+RestartFieldKeyUniqIdGap), meta.GetDiskKeyUniqId())

	meta.Close()

	meta, err = OpenMeta(testDir)
	require.NoError(t, err)
	require.Equal(t, uint64(110000+RestartFieldKeyUniqIdGap*2), meta.GetCurrentKeyUniqId())
	require.Equal(t, uint64(110000+RestartFieldKeyUniqIdGap*2), meta.GetDiskKeyUniqId())
	meta.Close()
	meta, err = OpenMeta(testDir)
	require.NoError(t, err)
	require.Equal(t, uint64(110000+RestartFieldKeyUniqIdGap*3), meta.GetCurrentKeyUniqId())
	require.Equal(t, uint64(110000+RestartFieldKeyUniqIdGap*3), meta.GetDiskKeyUniqId())
	meta.Close()
}

func TestMeta_FlushIndex(t *testing.T) {
	if err := os.MkdirAll(testDir, 0755); err != nil {
		panic(err)
	}
	defer os.RemoveAll(testDir)

	config.GlobalConfig.Bitalos.EnableRaftlogRestore = true
	index, e := OpenMeta(testDir)
	require.NoError(t, e)

	require.Equal(t, uint64(0), index.GetFlushIndex())

	index.SetFlushIndex(100)
	require.Equal(t, uint64(100), index.GetFlushIndex())

	index.Close()

	index, e = OpenMeta(testDir)
	require.NoError(t, e)
	require.Equal(t, uint64(100), index.GetFlushIndex())
	index.Close()
}
