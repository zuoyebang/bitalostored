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
	"errors"
	"runtime/debug"
	"strconv"

	"github.com/zuoyebang/bitalostored/stored/engine/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/cmd"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
)

func init() {
	AddCommand(map[string]*Cmd{
		"compact":      {Sync: false, Handler: compactCommand, NoKey: true},
		"keyslot":      {Sync: false, Handler: keyslotCommand, NoKey: true},
		"keyuniqid":    {Sync: false, Handler: keyUniqIdCommand, NoKey: true},
		"debuginfo":    {Sync: false, Handler: debugInfoCommand, NoKey: true},
		"cacheinfo":    {Sync: false, Handler: cacheInfoCommand, NoKey: true},
		"diskinfo":     {Sync: false, Handler: diskInfoCommand, NoKey: true},
		"freememory":   {Sync: false, Handler: freeOsMemoryCommand, NoKey: true},
		"vtablegc":     {Sync: false, Handler: vtableGCCommand, NoKey: true},
		"flushbitpage": {Sync: false, Handler: flushBitpageCommand, NoKey: true},
		"flushdb":      {Sync: false, Handler: flushDBCommand, NoKey: true},
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
	slotId := btools.GetSlotId(c.KeyHash)
	c.Writer.WriteInteger(int64(slotId))
	return nil
}

func keyUniqIdCommand(c *Client) error {
	id := c.DB.GetCurrentKeyUniqId()
	c.Writer.WriteInteger(int64(id))
	return nil
}

func compactCommand(c *Client) error {
	c.DB.Compact()
	c.Writer.WriteStatus("OK")
	return nil
}

func vtableGCCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return errn.CmdParamsErr(resp.TYPE)
	}
	slotId, err := extend.ParseUint16(string(args[0]))
	if err != nil {
		return err
	}

	c.DB.VtableGC(slotId)
	c.Writer.WriteStatus("OK")
	return nil
}

func flushDBCommand(c *Client) error {
	flushForce := false
	if len(c.Args) == 1 && string(c.Args[0]) == "flushForce" {
		flushForce = true
	}

	c.DB.FlushDB(flushForce)
	c.Writer.WriteStatus("OK")
	return nil
}

func flushBitpageCommand(c *Client) error {
	c.DB.FlushBitpage()
	c.Writer.WriteStatus("OK")
	return nil
}

func debugInfoCommand(c *Client) error {
	info := c.DB.DebugInfo()
	c.Writer.WriteBulk(info)
	return nil
}

func diskInfoCommand(c *Client) error {
	info := c.DB.DirDiskInfo()
	c.Writer.WriteBulk(info)
	return nil
}

func cacheInfoCommand(c *Client) error {
	info := c.DB.CacheInfo()
	c.Writer.WriteBulk(info)
	return nil
}

func rmDBBitupleCommand(c *Client) error {
	if len(c.Args) < 3 {
		return errn.ErrArgsEmpty
	}

	var err error
	var slotBegin, slotEnd uint64
	slotBegin, err = strconv.ParseUint(unsafe2.String(c.Args[0]), 10, 64)
	if err != nil {
		return errn.ErrSyntax
	}
	slotEnd, err = strconv.ParseUint(unsafe2.String(c.Args[1]), 10, 64)
	if err != nil {
		return errn.ErrSyntax
	}
	if slotEnd < slotBegin || slotEnd >= uint64(btools.TotalSlot) {
		return errors.New("slot not allowed")
	}

	if !cmd.CheckCmdToken(unsafe2.String(c.Args[2])) {
		return errn.ErrCmdToken
	}

	for i := slotBegin; i <= slotEnd; i++ {
		if err = c.DB.RemoveSlot(uint16(i)); err != nil {
			log.Errorf("execute to remove failed slot:%d err:%v", i, err)
		} else {
			log.Infof("execute to remove success slot:%d", i)
		}
	}

	log.Infof("remove bitalosdb slot success begin:%d end:%d", slotBegin, slotEnd)
	c.Writer.WriteStatus(resp.ReplyOK)
	return nil
}
