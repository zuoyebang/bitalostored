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

package rstring

import (
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitskv"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
)

func (so *StringObject) GetLuaScript(key []byte) ([]byte, func()) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, nil
	}

	ek, kcloser := base.EncodeMetaKeyForLua(key)
	defer kcloser()

	script, _, vcloser, err := so.getValueCheckAliveForString(ek)

	if err != nil {
		return nil, nil
	} else {
		return script, vcloser
	}
}

func (so *StringObject) ExistsLuaScript(key []byte) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	ek, kcloser := base.EncodeMetaKeyForLua(key)
	defer kcloser()

	mkv, err := so.BaseDb.BaseGetMetaWithoutValue(ek)
	if mkv == nil || err != nil {
		return 0, err
	}
	defer base.PutMkvToPool(mkv)

	if !mkv.IsAlive() {
		return 0, nil
	}

	return 1, nil
}

func (so *StringObject) SetLuaScript(key, script []byte) error {
	if err := btools.CheckValueSize(script); err != nil {
		return err
	}

	ek, ekCloser := base.EncodeMetaKeyForLua(key)
	defer ekCloser()
	return so.setValueForString(ek, script, 0)
}

func (so *StringObject) FlushLuaScript() error {
	wb := so.BaseDb.DB.GetMetaWriteBatchFromPool()
	defer so.BaseDb.DB.PutWriteBatchToPool(wb)

	minKey, kcloser := base.EncodeMetaKeyForLua(nil)
	defer kcloser()

	iterOpts := &bitskv.IterOptions{
		SlotId: uint32(btools.LuaScriptSlot),
	}

	it := so.BaseDb.DB.NewIteratorMeta(iterOpts)
	defer it.Close()

	var n int64
	for it.Seek(minKey); it.Valid() && it.ValidForPrefix(minKey); it.Next() {
		n++
		_ = wb.Delete(it.RawKey())
		if so.BaseDb.MetaCache != nil {
			so.BaseDb.MetaCache.Delete(it.RawKey())
		}
	}

	if n > 0 {
		return wb.Commit()
	}

	return nil
}

func (so *StringObject) LuaScriptLen() int64 {
	minKey, kcloser := base.EncodeMetaKeyForLua(nil)
	defer kcloser()

	iterOpts := &bitskv.IterOptions{
		SlotId: uint32(btools.LuaScriptSlot),
	}

	it := so.BaseDb.DB.NewIteratorMeta(iterOpts)
	defer it.Close()

	var n int64
	for it.Seek(minKey); it.Valid() && it.ValidForPrefix(minKey); it.Next() {
		n++
	}

	return n
}
