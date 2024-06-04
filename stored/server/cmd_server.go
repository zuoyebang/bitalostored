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

package server

import (
	"time"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
)

func init() {
	AddCommand(map[string]*Cmd{
		resp.PING: {Sync: false, Handler: pingCommand, NoKey: true},
		resp.ECHO: {Sync: false, Handler: echoCommand, NoKey: true},
		resp.TIME: {Sync: false, Handler: timeCommand, NoKey: true},
	})
}

func pingCommand(c *Client) error {
	c.RespWriter.WriteStatus(resp.ReplyPONG)
	return nil
}

func echoCommand(c *Client) error {
	if len(c.Args) != 1 {
		return resp.CmdParamsErr(resp.ECHO)
	}

	c.RespWriter.WriteBulk(c.Args[0])
	return nil
}

func timeCommand(c *Client) error {
	if len(c.Args) != 0 {
		return resp.CmdParamsErr(resp.TIME)
	}

	t := time.Now()

	s := t.Unix()
	n := t.UnixNano()
	m := (n - s*1e9) / 1e3

	ay := []interface{}{
		extend.FormatInt64ToSlice(s),
		extend.FormatInt64ToSlice(m),
	}

	c.RespWriter.WriteArray(ay)
	return nil
}
