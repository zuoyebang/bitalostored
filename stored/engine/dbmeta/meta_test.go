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

package dbmeta

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

const testDir = "./test"

func TestMeta(t *testing.T) {
	require.NoError(t, os.MkdirAll(testDir, 0755))
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

	require.Equal(t, uint64(5), index.GetSnapshotOrder())
	require.Equal(t, uint64(60), index.GetSnapshotIndex())

	index.ClearSnapshot()

	require.Equal(t, uint64(0), index.GetSnapshotOrder())
	require.Equal(t, uint64(0), index.GetSnapshotIndex())
	require.Equal(t, int64(0), index.GetSnapshotStamp())
}

func TestMeta_Bitalosdb_CompressType_0(t *testing.T) {
	require.NoError(t, os.MkdirAll(testDir, 0755))
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
	require.NoError(t, os.MkdirAll(testDir, 0755))
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

func TestMeta_Bitalosdb_StoreKey_1(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)

	index, e := OpenMeta(testDir)
	require.NoError(t, e)

	isSet, cType := index.GetBitalosdbStoreKeyCfg()
	require.Equal(t, false, isSet)
	require.Equal(t, false, cType)

	index.SetBitalosdbStoreKeyCfg(1)

	isSet, cType = index.GetBitalosdbStoreKeyCfg()
	require.Equal(t, true, isSet)
	require.Equal(t, true, cType)

	index.Close()

	index, e = OpenMeta(testDir)
	require.NoError(t, e)

	isSet, cType = index.GetBitalosdbStoreKeyCfg()
	require.Equal(t, true, isSet)
	require.Equal(t, true, cType)
}

func TestMeta_Bitalosdb_StoreKey_2(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)

	index, e := OpenMeta(testDir)
	require.NoError(t, e)

	isSet, cType := index.GetBitalosdbStoreKeyCfg()
	require.Equal(t, false, isSet)
	require.Equal(t, false, cType)

	index.SetBitalosdbStoreKeyCfg(2)

	isSet, cType = index.GetBitalosdbStoreKeyCfg()
	require.Equal(t, true, isSet)
	require.Equal(t, false, cType)

	index.Close()

	index, e = OpenMeta(testDir)
	require.NoError(t, e)

	isSet, cType = index.GetBitalosdbStoreKeyCfg()
	require.Equal(t, true, isSet)
	require.Equal(t, false, cType)
}

func TestMeta_KeyId(t *testing.T) {
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)
	meta, err := OpenMeta(testDir)
	require.NoError(t, err)

	require.Equal(t, uint64(RestartFieldKeyUniqIdGap), meta.GetCurrentKeyUniqId())
	require.Equal(t, uint64(RestartFieldKeyUniqIdGap), meta.GetDiskKeyUniqId())

	ids := make([]uint64, 100000)
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
	require.NoError(t, os.MkdirAll(testDir, 0755))
	defer os.RemoveAll(testDir)

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
