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
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

func init() {
	AddCommand(map[string]*Cmd{
		resp.TYPE:      {Sync: resp.IsWriteCmd(resp.TYPE), Handler: typeCommand},
		resp.DEL:       {Sync: resp.IsWriteCmd(resp.DEL), Handler: delCommand, KeySkip: 1},
		resp.TTL:       {Sync: resp.IsWriteCmd(resp.TTL), Handler: ttlCommand},
		resp.PTTL:      {Sync: resp.IsWriteCmd(resp.PTTL), Handler: pttlCommand},
		resp.EXISTS:    {Sync: resp.IsWriteCmd(resp.EXISTS), Handler: existsCommand},
		resp.EXPIRE:    {Sync: resp.IsWriteCmd(resp.EXPIRE), Handler: expireCommand},
		resp.EXPIREAT:  {Sync: resp.IsWriteCmd(resp.EXPIREAT), Handler: expireAtCommand},
		resp.PEXPIRE:   {Sync: resp.IsWriteCmd(resp.PEXPIRE), Handler: pexpireCommand},
		resp.PEXPIREAT: {Sync: resp.IsWriteCmd(resp.PEXPIREAT), Handler: pexpireAtCommand},
		resp.PERSIST:   {Sync: resp.IsWriteCmd(resp.PERSIST), Handler: persistCommand},
		resp.INFO:      {Sync: false, Handler: infoCommand, NoKey: true},
	})
}

func typeCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return errn.CmdParamsErr(resp.TYPE)
	}

	if t, err := c.DB.Type(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.Writer.WriteStatus(t)
		return nil
	}
}

func delCommand(c *Client) error {
	args := c.Args
	argsLen := len(args)
	if argsLen == 0 {
		return errn.CmdParamsErr(resp.DEL)
	}

	n, err := c.DB.Del(c.KeyHash, args...)
	if err != nil {
		return err
	}
	c.Writer.WriteInteger(n)
	return nil
}

func expireCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return errn.CmdParamsErr(resp.EXPIRE)
	}

	duration, err := utils.ByteToInt64(args[1])
	if err != nil {
		return errn.ErrValue
	}

	var n int64
	n, err = c.DB.Expire(args[0], c.KeyHash, duration)
	if err != nil {
		return err
	}
	c.Writer.WriteInteger(n)
	return nil
}

func expireAtCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return errn.CmdParamsErr(resp.EXPIREAT)
	}

	when, err := utils.ByteToInt64(args[1])
	if err != nil {
		return errn.ErrValue
	}

	var n int64
	n, err = c.DB.ExpireAt(args[0], c.KeyHash, when)
	if err != nil {
		return err
	}
	c.Writer.WriteInteger(n)
	return nil
}

func pexpireCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return errn.CmdParamsErr(resp.EXPIRE)
	}

	duration, err := utils.ByteToInt64(args[1])
	if err != nil {
		return errn.ErrValue
	}

	var n int64
	n, err = c.DB.PExpire(args[0], c.KeyHash, duration)
	if err != nil {
		return err
	}
	c.Writer.WriteInteger(n)
	return nil
}

func pexpireAtCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return errn.CmdParamsErr(resp.EXPIREAT)
	}

	when, err := utils.ByteToInt64(args[1])
	if err != nil {
		return errn.ErrValue
	}

	var n int64
	n, err = c.DB.PExpireAt(args[0], c.KeyHash, when)
	if err != nil {
		return err
	}
	c.Writer.WriteInteger(n)
	return nil
}

func existsCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return errn.CmdParamsErr(resp.EXISTS)
	}

	if n, err := c.DB.Exists(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
		return nil
	}
}

func ttlCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return errn.CmdParamsErr(resp.TTL)
	}

	if n, err := c.DB.TTl(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
		return nil
	}
}

func pttlCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return errn.CmdParamsErr(resp.PTTL)
	}

	if n, err := c.DB.PTTl(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
		return nil
	}
}

func persistCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return errn.CmdParamsErr(resp.PERSIST)
	}

	if n, err := c.DB.Persist(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}
	return nil
}

func infoCommand(c *Client) error {
	var info []byte
	sinfo := c.GetInfo()
	var closer func()
	if len(c.Args) == 0 {
		info, closer = sinfo.Marshal()
	} else {
		switch unsafe2.String(c.Args[0]) {
		case "server":
			info, closer = sinfo.Server.Marshal()
		case "clients":
			info, closer = sinfo.Client.Marshal()
		case "clusterinfo":
			info, closer = sinfo.Cluster.Marshal()
		case "stats":
			info, closer = sinfo.Stats.Marshal()
		case "_leader_address":
			info = []byte(sinfo.Cluster.LeaderAddress)
		case "_server_address":
			info = []byte(sinfo.Server.ServerAddress)
		}
	}
	c.Writer.WriteBulk(info)
	if closer != nil {
		closer()
	}
	return nil
}
