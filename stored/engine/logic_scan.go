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

import "github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"

func (b *Bitalos) Scan(cursor []byte, count int, match string, dt btools.DataType) ([]byte, [][]byte, error) {
	return b.bitsdb.Scan(cursor, count, match, dt)
}

func (b *Bitalos) ScanSlotId(slotId uint32, cursor []byte, count int, match string) ([]byte, [][]byte, error) {
	return b.bitsdb.ScanSlotId(slotId, cursor, count, match)
}

func (b *Bitalos) HScan(key []byte, khash uint32, cursor []byte, count int, match string) ([]byte, []btools.FVPair, error) {
	return b.bitsdb.HashObj.HScan(key, khash, cursor, count, match)
}

func (b *Bitalos) SScan(key []byte, khash uint32, cursor []byte, count int, match string) ([]byte, [][]byte, error) {
	return b.bitsdb.SetObj.SScan(key, khash, cursor, count, match)
}

func (b *Bitalos) ZScan(key []byte, khash uint32, cursor []byte, count int, match string) ([]byte, []btools.ScorePair, error) {
	return b.bitsdb.ZsetObj.ZScan(key, khash, cursor, count, match)
}
