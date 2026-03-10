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

package btools

import "github.com/zuoyebang/bitalostored/butils/hash"

const (
	TotalSlot       uint32 = 1024
	TotalSlotMask          = TotalSlot - 1
	LuaScriptSlotId uint16 = 2048

	SlotRemoveStart    uint8 = 1
	SlotRemoveFinished uint8 = 2
)

func GetSlotId(khash uint32) uint16 {
	return uint16(khash & TotalSlotMask)
}

func GetKeySlotId(key []byte) uint32 {
	return hash.Fnv32(key) & TotalSlotMask
}
