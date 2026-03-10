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
	"testing"

	"github.com/zuoyebang/bitalostored/stored/engine/btools"

	"github.com/stretchr/testify/require"
)

func TestLuaCmd(t *testing.T) {
	bdb := testNewNoCacheBitsDB()
	defer bdb.Close()

	kn := 1
	kkn := 100
	itemList := testMakeItemList(kn, kkn, btools.DataTypeHash, 20, 100)
	fields := itemList[0].kvs
	for i := 0; i < kkn; i += 2 {
		require.NoError(t, bdb.db.SetLuaScript(fields[i], fields[i+1]))
	}

	size := bdb.db.LuaScriptLen()
	require.Equal(t, int64(kkn/2), size)

	for i := 0; i < kkn; i += 2 {
		v, vcloser := bdb.db.GetLuaScript(fields[i])
		require.Equal(t, fields[i+1], v)
		vcloser()
		exist, err := bdb.db.ExistsLuaScript(fields[i])
		require.NoError(t, err)
		require.Equal(t, int64(1), exist)
		exist, _ = bdb.db.ExistsLuaScript(fields[i+1])
		require.Equal(t, int64(0), exist)
	}

	require.NoError(t, bdb.db.FlushLuaScript())
	size = bdb.db.LuaScriptLen()
	require.Equal(t, int64(0), size)
}
