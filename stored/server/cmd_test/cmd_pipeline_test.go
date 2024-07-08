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

package cmd_test

import (
	"reflect"
	"testing"
)

func TestPipeline(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	key1 := []byte("testpipekey1")
	val1 := testRandBytes(10 << 10)
	key2 := []byte("testpipekey2")
	val2 := testRandBytes(10 << 10)
	key3 := []byte("testpipekey3")
	val3 := testRandBytes(10 << 10)

	testCommands := []struct {
		args     []interface{}
		expected interface{}
	}{
		{
			[]interface{}{"SET", key1, val1},
			"OK",
		},
		{
			[]interface{}{"SET", key2, val2},
			"OK",
		},
		{
			[]interface{}{"SET", key3, val3},
			"OK",
		},
		{
			[]interface{}{"GET", key1},
			val1,
		},
		{
			[]interface{}{"GET", key2},
			val2,
		},
		{
			[]interface{}{"GET", key3},
			val3,
		},
	}

	for _, cmd := range testCommands {
		if err := c.Send(cmd.args[0].(string), cmd.args[1:]...); err != nil {
			t.Fatalf("Send(%v) returned error %v", cmd.args, err)
		}
	}
	if err := c.Flush(); err != nil {
		t.Errorf("Flush() returned error %v", err)
	}
	for _, cmd := range testCommands {
		actual, err := c.Receive()
		if err != nil {
			t.Fatalf("Receive(%v) returned error %v", cmd.args, err)
		}
		if !reflect.DeepEqual(actual, cmd.expected) {
			t.Errorf("Receive(%v) = %v, want %v", cmd.args, actual, cmd.expected)
		}
	}
}
