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
	"fmt"
	"strconv"

	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"

	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

func migrateSlots(c *Client) error {
	if len(c.Args) < 3 {
		return resp.CmdParamsErr("migrateslots")
	}
	slot, e := strconv.ParseUint(string(c.Args[2]), 10, 32)
	if e != nil {
		return e
	}

	host := fmt.Sprintf("%s:%s", string(c.Args[0]), string(c.Args[1]))
	if _, e := c.DB.MigrateStart(c.server.address, host, uint32(slot), c.server.IsMaster, c.server.MigrateDelToSlave); e != nil {
		log.Warn("migrate error tohost: ", host, " slots: ", slot, " error: ", e)
		return e
	}

	c.RespWriter.WriteStatus("OK")
	return nil
}

func migrateStatus(c *Client) error {
	if len(c.Args) > 1 {
		if u, e := strconv.ParseUint(string(c.Args[1]), 10, 64); e != nil {
			return e
		} else if u != c.DB.Meta.GetMigrateSlotid() {
			return errn.ErrSlotIdNotMatch
		}
	}

	if c.DB.Migrate != nil {
		c.RespWriter.WriteStatus(c.DB.Migrate.Info())
	} else {
		c.RespWriter.WriteStatus("{}")
	}
	return nil
}

func migrateEnd(c *Client) error {
	if len(c.Args) < 1 {
		return resp.CmdParamsErr("migrateend")
	}
	slot, e := strconv.ParseUint(string(c.Args[0]), 10, 32)
	if e != nil {
		return e
	}

	if e := c.DB.MigrateOver(slot); e != nil {
		return e
	}

	c.RespWriter.WriteStatus("OK")
	return nil
}

func init() {
	AddCommand(map[string]*Cmd{
		"migrateslots":  {Sync: true, Name: "migrateslots host port slotid...", Handler: migrateSlots},
		"migratestatus": {Sync: false, Name: "migratestatus slotid", Handler: migrateStatus},
		"migrateend":    {Sync: true, Name: "migrateend slotid", Handler: migrateEnd},
	})
}
