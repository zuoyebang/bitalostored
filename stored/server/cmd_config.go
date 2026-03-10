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
	"strconv"
	"strings"

	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/internal/cmd"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
)

const (
	CONFIGGET = "GET"
	CONFIGSET = "SET"
)

func init() {
	AddCommand(map[string]*Cmd{
		resp.CONFIG: {Sync: false, Handler: configCommand, NoKey: true},
	})
}

func configCommand(c *Client) error {
	args := c.Args
	if len(args) < 3 {
		return errn.CmdParamsErr(resp.CONFIG)
	}

	op := strings.ToUpper(unsafe2.String(args[0]))
	if op != CONFIGSET {
		return errn.ErrNotImplement
	}

	configName := strings.ToLower(unsafe2.String(args[1]))
	configValue, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return err
	}

	switch configName {
	case cmd.ConfigSetDBAUTOCOMPACT:
		return configSetDBAutoCompact(c, configValue)
	case cmd.ConfigSetRaftFullSync:
		if len(args) != 4 || !cmd.CheckCmdToken(string(args[3])) {
			return errn.ErrCmdToken
		}
		return configSetRaftFullSync(c, configValue)
	case cmd.ConfigSetRaftLogCompact:
		if len(args) != 4 || !cmd.CheckCmdToken(string(args[3])) {
			return errn.ErrCmdToken
		}
		return configSetRaftLogCompact(c, configValue)
	default:
		return errn.ErrNotImplement
	}
}

func configSetDBAutoCompact(c *Client, value int) error {
	db := c.server.GetDB()
	if db != nil {
		if value == 1 {
			db.SetAutoCompact(true)
			c.server.Info.Server.AutoCompact = true
		} else {
			db.SetAutoCompact(false)
			c.server.Info.Server.AutoCompact = false
		}
		c.server.Info.Server.UpdateCache()
		c.Writer.WriteStatus(resp.ReplyOK)
	}
	return nil
}

func configSetRaftFullSync(c *Client, on int) error {
	if c.server.IsWitness || !c.server.isOpenRaft {
		return nil
	}

	newValue := false
	if on == 1 {
		newValue = true
	}

	oldValue := c.server.Info.Server.ForbidRaftFullSync
	c.server.Info.Server.ForbidRaftFullSync = newValue
	c.server.SetRaftNodeState(cmd.ConfigSetRaftFullSync, newValue)
	if oldValue != newValue {
		c.server.Info.Server.UpdateCache()
	}

	log.Infof("[CMD] config set raft full sync %v to %v success", oldValue, newValue)
	c.Writer.WriteStatus(resp.ReplyOK)
	return nil
}

func configSetRaftLogCompact(c *Client, on int) error {
	if c.server.IsWitness || !c.server.isOpenRaft {
		return nil
	}

	newValue := false
	if on == 1 {
		newValue = true
	}

	oldValue := c.server.Info.Server.ForbidRaftLogCompact
	c.server.Info.Server.ForbidRaftLogCompact = newValue
	c.server.SetRaftNodeState(cmd.ConfigSetRaftLogCompact, newValue)
	if oldValue != newValue {
		c.server.Info.Server.UpdateCache()
	}

	log.Infof("[CMD] config set raft log compact %v to %v success", oldValue, newValue)
	c.Writer.WriteStatus(resp.ReplyOK)
	return nil
}
