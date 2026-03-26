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

package models

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

var client Client

func init() {
	var err error
	coordinator := "sqlite"
	if client, err = NewClient(coordinator, nil); err != nil {
		fmt.Println("init Client err:", err.Error())
	}
}

func TestCreateAndGetAnd(t *testing.T) {
	path := "TestCreateAndGet"
	val := []byte("TestCreateAndGet")
	err := client.Delete(path)
	assert.NoError(t, err)
	client.Create(path, val)
	data, err := client.Read(path)
	assert.NoError(t, err)
	assert.Equal(t, val, data)
}

func TestCreateListAndList(t *testing.T) {
	path := "TestCreateListAndList"
	path1 := "TestCreateListAndList/one"
	path2 := "TestCreateListAndList/two"
	val1 := []byte("TestCreateListAndList-1")
	val2 := []byte("TestCreateListAndList-2")
	err := client.Delete(path1)
	assert.NoError(t, err)
	err = client.Delete(path2)
	assert.NoError(t, err)
	client.Create(path1, val1)
	client.Create(path2, val2)
	data, err := client.List(path)
	assert.Equal(t, val1, []byte(data[0]))
	assert.Equal(t, val2, []byte(data[1]))
}

func TestCreateAndUpdate(t *testing.T) {
	path := "TestCreateAndUpdate"
	val := []byte("TestCreateAndUpdate")
	err := client.Delete(path)
	assert.NoError(t, err)
	client.Create(path, val)
	data, err := client.Read(path)
	assert.NoError(t, err)
	assert.Equal(t, val, data)
	newval := []byte("TestCreateAndUpdate-new")
	err = client.Update(path, newval, false)
	assert.NoError(t, err)
}
