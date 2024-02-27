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

	"github.com/zuoyebang/bitalostored/proxy/resp"

	"github.com/gomodule/redigo/redis"

	"github.com/stretchr/testify/assert"
)

func TestServerCommand(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	v, err := redis.String(c.Do("ping"))
	assert.NoError(t, err)
	assert.Equal(t, resp.ReplyPONG, v)

	v, err = redis.String(c.Do("echo", "foo"))
	assert.NoError(t, err)
	assert.Equal(t, "foo", v)
}

func TestServerCommandErrorParams(t *testing.T) {
	c := getTestConn()
	defer c.Close()

	_, err := c.Do("echo")
	assert.Error(t, err)
	assert.Equal(t, resp.CmdParamsErr("ECHO").Error(), err.Error())
}
