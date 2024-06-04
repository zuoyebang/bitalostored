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

package server

import (
	"os"
	"runtime/debug"
	"syscall"

	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

func init() {
	AddCommand(map[string]*Cmd{
		"compact":    {Sync: false, Handler: compactCommand, NoKey: true},
		"delexpire":  {Sync: false, Handler: delExpireCommand, NoKey: true},
		"keyslot":    {Sync: false, Handler: keyslotCommand, NoKey: true},
		"keyuniqid":  {Sync: false, Handler: keyUniqIdCommand, NoKey: true},
		"debuginfo":  {Sync: false, Handler: debugInfoCommand, NoKey: true},
		"cacheinfo":  {Sync: false, Handler: cacheInfoCommand, NoKey: true},
		"freememory": {Sync: false, Handler: freeOsMemoryCommand, NoKey: true},
		"shutdown":   {Sync: false, Handler: shutdownCommand, NoKey: true},
	})
}

func shutdownCommand(c *Client) error {
	c.conn.Close()

	p, _ := os.FindProcess(os.Getpid())

	p.Signal(syscall.SIGTERM)
	p.Signal(os.Interrupt)

	return errn.ErrClientQuit
}

func freeOsMemoryCommand(c *Client) error {
	debug.FreeOSMemory()
	c.RespWriter.WriteStatus(resp.ReplyOK)
	return nil
}

func keyslotCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.TYPE)
	}
	slotId := c.KeyHash % utils.TotalSlot
	c.RespWriter.WriteInteger(int64(slotId))
	return nil
}

func keyUniqIdCommand(c *Client) error {
	id := c.DB.Meta.GetCurrentKeyUniqId()
	c.RespWriter.WriteInteger(int64(id))
	return nil
}

func compactCommand(c *Client) error {
	c.DB.Compact()
	c.RespWriter.WriteStatus("OK")
	return nil
}

func debugInfoCommand(c *Client) error {
	info := c.DB.DebugInfo()
	c.RespWriter.WriteBulk(info)
	return nil
}

func cacheInfoCommand(c *Client) error {
	info := c.DB.CacheInfo()
	c.RespWriter.WriteBulk(info)
	return nil
}

func delExpireCommand(c *Client) error {
	c.DB.ScanDelExpireAsync()
	c.RespWriter.WriteStatus("OK")
	return nil
}
