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
	"errors"
	"fmt"

	"github.com/gomodule/redigo/redis"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
)

func init() {
	AddCommand(map[string]*Cmd{
		resp.DBSYNC:        {Sync: false, Handler: dbsyncCommand, NoKey: true},
		resp.DBSYNCCONNECT: {Sync: false, Handler: dbsyncconnectCommand, NoKey: true},
	})
}

func dbsyncCommand(c *Client) error {
	if len(c.Args) != 3 {
		return resp.CmdParamsErr(resp.DBSYNC)
	}
	token := string(c.Args[0])
	if token != config.GlobalConfig.Server.Token {
		return errors.New("valid token err")
	}

	ip := string(c.Args[1])
	port := string(c.Args[2])
	if len(ip) <= 0 || len(port) <= 0 {
		return errors.New("valid ip/port err")
	}

	host := fmt.Sprintf("%s:%s", ip, port)
	rs, err := redis.Dial("tcp", host)

	defer rs.Close()

	if err != nil {
		return err
	}

	if addr, err := redis.String(rs.Do(resp.DBSYNCCONNECT, token)); err != nil {
		c.server.Info.Stats.DbSyncErr = err.Error()
		c.server.Info.Stats.DbSyncStatus = DB_SYNC_NOTHING
		c.RespWriter.WriteError(err)
		return err
	} else {
		c.server.buildDbAsyncConn(addr)
		c.RespWriter.WriteStatus(resp.ReplyOK)
	}
	return nil
}

func dbsyncconnectCommand(c *Client) error {
	if len(c.Args) != 1 {
		return resp.CmdParamsErr(resp.DBSYNCCONNECT)
	}
	if string(c.Args[0]) != config.GlobalConfig.Server.Token {
		return errors.New("valid token err")
	}

	if addr, err := c.server.buildDbSyncListener(); err != nil {
		c.RespWriter.WriteError(err)
		return err
	} else {
		c.RespWriter.WriteBulk([]byte(addr))
	}

	return nil
}
