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
	"os"
	"syscall"
	"time"

	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"

	"github.com/zuoyebang/bitalostored/butils/extend"
)

func init() {
	AddCommand(map[string]*Cmd{
		resp.PING:     {Sync: false, Handler: pingCommand, NoKey: true},
		resp.ECHO:     {Sync: false, Handler: echoCommand, NoKey: true},
		resp.TIME:     {Sync: false, Handler: timeCommand, NoKey: true},
		resp.SHUTDOWN: {Sync: false, Handler: shutdownCommand, NoKey: true},
	})
}

func pingCommand(c *Client) error {
	c.Writer.WriteStatus(resp.ReplyPONG)
	return nil
}

func echoCommand(c *Client) error {
	if len(c.Args) != 1 {
		return errn.CmdParamsErr(resp.ECHO)
	}

	c.Writer.WriteBulk(c.Args[0])
	return nil
}

func timeCommand(c *Client) error {
	if len(c.Args) != 0 {
		return errn.CmdParamsErr(resp.TIME)
	}

	t := time.Now()

	s := t.Unix()
	n := t.UnixNano()
	m := (n - s*1e9) / 1e3

	ay := []interface{}{
		extend.FormatInt64ToSlice(s),
		extend.FormatInt64ToSlice(m),
	}

	c.Writer.WriteArray(ay)
	return nil
}

func shutdownCommand(c *Client) error {
	p, _ := os.FindProcess(os.Getpid())
	p.Signal(syscall.SIGTERM)
	p.Signal(os.Interrupt)

	c.Writer.WriteStatus(resp.ReplyOK)
	return nil
}
