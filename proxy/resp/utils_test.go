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

package resp

import (
	"fmt"
	"testing"
	"time"
)

func TestPackArgs(t *testing.T) {
	args := make([][]byte, 20)
	for i := 0; i < 20; i++ {
		args[i] = []byte("abc")
	}

	packByte := func(args [][]byte, loop int) {
		start := time.Now()
		var params []interface{}
		_ = params
		for i := 0; i < loop; i++ {
			params = PackArgs(args)
			if i == 0 {
				fmt.Printf("%+v\n", params)
			}
		}
		fmt.Printf("packArgs: %s\n", time.Now().Sub(start))
	}
	makeSlice := func(args [][]byte, loop int) {
		start := time.Now()
		for i := 0; i < loop; i++ {
			params2 := make([]interface{}, len(args), len(args))
			for j := 0; j < len(args); j++ {
				params2[j] = args[j]
			}
			if i == 0 {
				fmt.Printf("%+v\n", params2)
			}
		}
		fmt.Printf("convert []interface{}: %s\n", time.Now().Sub(start))
	}
	loop := 1000000
	packByte(args, loop)
	makeSlice(args, loop)

	argsString := make([]string, 20)
	for i := 0; i < 20; i++ {
		argsString[i] = "abc"
	}
	packString := func(args []string, loop int) {
		start := time.Now()
		var params []interface{}
		_ = params
		for i := 0; i < loop; i++ {
			params = PackArgs(args)
			if i == 0 {
				fmt.Printf("%+v\n", params)
			}
		}
		fmt.Printf("packArgs: %s\n", time.Now().Sub(start))
	}
	makeSliceString := func(args []string, loop int) {
		start := time.Now()
		for i := 0; i < loop; i++ {
			params2 := make([]interface{}, len(args), len(args))
			for j := 0; j < len(args); j++ {
				params2[j] = args[j]
			}
			if i == 0 {
				fmt.Printf("%+v\n", params2)
			}
		}
		fmt.Printf("convert []interface{}: %s\n", time.Now().Sub(start))
	}
	packString(argsString, loop)
	makeSliceString(argsString, loop)
}
