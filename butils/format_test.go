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

package butils

import (
	"fmt"
	"testing"
)

func TestFmtSize(t *testing.T) {
	fmt.Println(FmtSize(1023))
	fmt.Println(FmtSize(1025))
	fmt.Println(FmtSize(MB + 221*KB))
	fmt.Println(FmtSize(GB*2 + 123*MB))
	fmt.Println(FmtSize(TB + 3*GB))
	fmt.Println(FmtSize(EB*3 + TB*2))
}
