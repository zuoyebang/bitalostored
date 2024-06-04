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

package list

import (
	"errors"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb/base"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/dbconfig"
)

const ListCopyMax = 10000
const ListReadMax = 10000

var errIndexOverflow = errors.New("ERR index overflow ")
var errMoveTooMany = errors.New("ERR list moved too many")
var ErrIndexOutOfRange = errors.New("ERR index out of range")
var ErrWriteNoSpace = errors.New("ERR list write no space")
var ErrNoSuchKey = errors.New("ERR no such key")

type ListObject struct {
	base.BaseObject
	lbkeys *lBlockKeys
}

func (lo *ListObject) Close() {
	lo.BaseObject.Close()
}

func NewListObject(baseDb *base.BaseDB, cfg *dbconfig.Config) *ListObject {
	lo := &ListObject{
		BaseObject: base.NewBaseObject(baseDb, cfg, btools.LIST),
	}
	lo.lbkeys = newLBlockKeys()
	return lo
}
