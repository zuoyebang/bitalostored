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
	"github.com/zuoyebang/bitalostored/stored/internal/glob"
)

func (b *Bitalos) Scan(cursor []byte, count int, match string, dt uint8) ([]byte, [][]byte, error) {
	var r glob.Glob
	var err error
	if len(match) > 0 {
		if match == "*" {
			match = ""
		} else {
			r, err = btools.BuildMatchRegexp(match)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	cmpFunc := func(member string) bool {
		if len(match) > 0 && !r.Match(member) {
			return false
		}
		return true
	}

	return b.bitsdb.DB.Scan(cursor, count, dt, cmpFunc)
}
