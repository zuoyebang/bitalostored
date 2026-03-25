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

package respcmd

import (
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

var DkKey = []byte("TestDk")

func TestDk(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	_, err := c.Do("dk.del", "77c44df457908f56", DkKey)
	if err != nil {
		t.Fatalf("dk.del key:%s err:%v", DkKey, err)
	}
	_, err = c.Do("dk.hcreate", DkKey, 2, "77c44df457908f56")
	if err != nil {
		t.Fatalf("dk.hcreate key:%s err:%v", DkKey, err)
	}
	_, err = c.Do("dk.hset", DkKey, "a", 1)
	if err != nil {
		t.Fatalf("dk.hset key:%s err:%v", DkKey, err)
	}

	n, err := redis.Int(c.Do("dk.exists", DkKey))
	if !assert.NoError(t, err) {
		return
	}
	if n != 1 {
		t.Fatal("dk.exists value empty")
	}
}
