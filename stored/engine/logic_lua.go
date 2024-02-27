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

func (b *Bitalos) GetLuaScript(key []byte) ([]byte, func()) {
	return b.bitsdb.StringObj.GetLuaScript(key)
}

func (b *Bitalos) ExistsLuaScript(key []byte) (int64, error) {
	return b.bitsdb.StringObj.ExistsLuaScript(key)
}

func (b *Bitalos) SetLuaScript(key []byte, script []byte) error {
	return b.bitsdb.StringObj.SetLuaScript(key, script)
}

func (b *Bitalos) FlushLuaScript() error {
	return b.bitsdb.StringObj.FlushLuaScript()
}

func (b *Bitalos) LuaScriptLen() int64 {
	return b.bitsdb.StringObj.LuaScriptLen()
}
