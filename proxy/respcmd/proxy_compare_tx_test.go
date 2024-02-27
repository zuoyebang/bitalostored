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

package respcmd

import (
	"testing"
)

func TestTxKv(t *testing.T) {
	cmds := kvTestCase
	for _, c := range cmds {
		switch len(c) {
		case 0, 1:
			continue
		default:
			checkTxResultIsSame(t, c[0], c[1:]...)
		}
	}
}

func TestTxHash(t *testing.T) {
	cmds := hashTestCase
	for _, c := range cmds {
		switch len(c) {
		case 0, 1:
			continue
		default:
			checkTxResultIsSame(t, c[0], c[1:]...)
		}
	}
}
func TestTxList(t *testing.T) {
	cmds := listTestCase
	for _, c := range cmds {
		switch len(c) {
		case 0, 1:
			continue
		default:
			checkTxResultIsSame(t, c[0], c[1:]...)
		}
	}
}

func TestTxSet(t *testing.T) {
	cmds := setTestCase
	for _, c := range cmds {
		switch len(c) {
		case 0, 1:
			continue
		default:
			checkTxResultIsSame(t, c[0], c[1:]...)
		}
	}
}

func TestTxZset(t *testing.T) {
	cmds := zsetTestCase
	for _, c := range cmds {
		switch len(c) {
		case 0, 1:
			continue
		default:
			checkTxResultIsSame(t, c[0], c[1:]...)
		}
	}
}
