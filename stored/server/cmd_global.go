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
	"encoding/binary"
	"runtime/debug"

	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/internal/tclock"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

func init() {
	AddCommand(map[string]*Cmd{
		"compact":       {Sync: false, Handler: compactCommand, NoKey: true},
		"compactexpire": {Sync: false, Handler: compactExpireCommand, NoKey: true},
		"compactbitree": {Sync: false, Handler: compactBitreeCommand, NoKey: true},
		"delexpire":     {Sync: false, Handler: delExpireCommand, NoKey: true},
		"keyslot":       {Sync: false, Handler: keyslotCommand, NoKey: true},
		"keyuniqid":     {Sync: false, Handler: keyUniqIdCommand, NoKey: true},
		"debuginfo":     {Sync: false, Handler: debugInfoCommand, NoKey: true},
		"cacheinfo":     {Sync: false, Handler: cacheInfoCommand, NoKey: true},
		"freememory":    {Sync: false, Handler: freeOsMemoryCommand, NoKey: true},
	})
}

func freeOsMemoryCommand(c *Client) error {
	debug.FreeOSMemory()
	c.Writer.WriteStatus(resp.ReplyOK)
	return nil
}

func keyslotCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return errn.CmdParamsErr(resp.TYPE)
	}
	slotId := c.KeyHash % utils.TotalSlot
	c.Writer.WriteInteger(int64(slotId))
	return nil
}

func keyUniqIdCommand(c *Client) error {
	id := c.DB.Meta.GetCurrentKeyUniqId()
	c.Writer.WriteInteger(int64(id))
	return nil
}

func compactCommand(c *Client) error {
	c.DB.Compact()
	c.Writer.WriteStatus("OK")
	return nil
}

func compactExpireCommand(c *Client) error {
	start := []byte{0}
	end := make([]byte, 8)
	binary.BigEndian.PutUint64(end, uint64(uint64(tclock.GetTimestampMilli())))
	c.DB.CompactExpire(start, end)
	c.Writer.WriteStatus("OK")
	return nil
}

func compactBitreeCommand(c *Client) error {
	c.DB.CompactBitree()
	c.Writer.WriteStatus("OK")
	return nil
}

func debugInfoCommand(c *Client) error {
	info := c.DB.DebugInfo()
	c.Writer.WriteBulk(info)
	return nil
}

func cacheInfoCommand(c *Client) error {
	info := c.DB.CacheInfo()
	c.Writer.WriteBulk(info)
	return nil
}

func delExpireCommand(c *Client) error {
	c.DB.ScanDelExpireAsync()
	c.Writer.WriteStatus("OK")
	return nil
}
