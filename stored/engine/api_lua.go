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
	"github.com/zuoyebang/bitalostored/stored/engine/btools"

	"github.com/zuoyebang/bitalostored/butils/hash"
	"github.com/gomodule/redigo/redis"
)

var (
	LuaKKVKey     = []byte{0xc, 0x94, 0xce, 0xca, 0xbf, 0xc4, 0xc6, 0x80, 0x6, 0xbe, 0x6b, 0x21, 0xac, 0xc8, 0xf2, 0xb7}
	LuaKKVKeyHash = hash.Fnv32(LuaKKVKey)
)

func (b *Bitalos) GetLuaScript(key []byte) ([]byte, func()) {
	if err := btools.CheckKeySize(key); err != nil {
		return nil, nil
	}

	script, closer, err := b.HGet(LuaKKVKey, LuaKKVKeyHash, key)
	if err != nil {
		return nil, nil
	}
	return script, closer
}

func (b *Bitalos) ExistsLuaScript(key []byte) (int64, error) {
	if err := btools.CheckKeySize(key); err != nil {
		return 0, err
	}

	script, closer, err := b.HGet(LuaKKVKey, LuaKKVKeyHash, key)
	defer func() {
		if closer != nil {
			closer()
		}
	}()
	if err != nil || len(script) == 0 {
		return 0, err
	}

	return 1, nil
}

func (b *Bitalos) SetLuaScript(key []byte, script []byte) error {
	if err := btools.CheckValueSize(script); err != nil {
		return err
	}

	_, err := b.HSet(LuaKKVKey, LuaKKVKeyHash, key, script)
	return err
}

func (b *Bitalos) FlushLuaScript() error {
	_, err := b.bitsdb.Del(LuaKKVKeyHash, LuaKKVKey)
	return err
}

func (b *Bitalos) LuaScriptLen() int64 {
	n, _ := b.HLen(LuaKKVKey, LuaKKVKeyHash)
	return n
}

func (b *Bitalos) MigrateLuaScript(dstHost string) error {
	unlockKey := b.bitsdb.LockKey(LuaKKVKeyHash)
	defer unlockKey()

	v, closer, err := b.HGetAll(LuaKKVKey, LuaKKVKeyHash)
	if err != nil {
		return err
	}
	defer func() {
		if closer != nil {
			closer()
		}
	}()

	if len(v) == 0 {
		return nil
	}

	var conn redis.Conn
	conn, err = redis.Dial("tcp", dstHost)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("hmset", LuaKKVKey, LuaKKVKeyHash, v)
	return err
}
