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
	"strconv"

	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

func init() {
	AddCommand(map[string]*Cmd{
		resp.SADD:        {Sync: resp.IsWriteCmd(resp.SADD), Handler: saddCommand},
		resp.SREM:        {Sync: resp.IsWriteCmd(resp.SREM), Handler: sremCommand},
		resp.SPOP:        {Sync: resp.IsWriteCmd(resp.SPOP), Handler: spopCommand},
		resp.SCARD:       {Sync: resp.IsWriteCmd(resp.SCARD), Handler: scardCommand},
		resp.SISMEMBER:   {Sync: resp.IsWriteCmd(resp.SISMEMBER), Handler: sismemberCommand},
		resp.SMEMBERS:    {Sync: resp.IsWriteCmd(resp.SMEMBERS), Handler: smembersCommand},
		resp.SRANDMEMBER: {Sync: resp.IsWriteCmd(resp.SRANDMEMBER), Handler: srandmemberCommand},

		resp.SCLEAR:     {Sync: resp.IsWriteCmd(resp.SCLEAR), Handler: sclearCommand, KeySkip: 1},
		resp.SEXPIRE:    {Sync: resp.IsWriteCmd(resp.SEXPIRE), Handler: sexpireCommand},
		resp.SEXPIREAT:  {Sync: resp.IsWriteCmd(resp.SEXPIREAT), Handler: sexpireAtCommand},
		resp.SPERSIST:   {Sync: resp.IsWriteCmd(resp.SPERSIST), Handler: spersistCommand},
		resp.STTL:       {Sync: resp.IsWriteCmd(resp.STTL), Handler: sttlCommand},
		resp.SKEYEXISTS: {Sync: resp.IsWriteCmd(resp.SKEYEXISTS), Handler: skeyexistsCommand},
	})
}

func saddCommand(c *Client) error {
	args := c.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.SADD)
	}

	if n, err := c.DB.SAdd(args[0], c.KeyHash, args[1:]...); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}

	return nil
}

func scardCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.SCARD)
	}

	if n, err := c.DB.SCard(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}

	return nil
}

func sismemberCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.SISMEMBER)
	}

	if n, err := c.DB.SIsMember(args[0], c.KeyHash, args[1]); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}

	return nil
}

func smembersCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.SMEMBERS)
	}

	res, err := c.DB.SMembers(args[0], c.KeyHash)
	if err != nil {
		return err
	}

	c.RespWriter.WriteSliceArray(res)
	return nil

}

func srandmemberCommand(c *Client) error {
	args := c.Args
	if len(args) < 1 {
		return resp.CmdParamsErr(resp.SRANDMEMBER)
	} else if len(args) > 2 {
		return resp.ErrSyntax
	}

	count := 1
	if len(args) == 2 {
		var err error
		if count, err = strconv.Atoi(string(args[1])); err != nil {
			return resp.ErrValue
		}
	}

	res, err := c.DB.SRandMember(args[0], c.KeyHash, int64(count))
	if err != nil {
		return err
	}
	if len(args) == 2 {
		c.RespWriter.WriteSliceArray(res)
	} else {
		if len(res) >= 1 {
			c.RespWriter.WriteBulk(res[0])
		} else {
			c.RespWriter.WriteBulk(nil)
		}
	}
	return nil

}

func sremCommand(c *Client) error {
	args := c.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.SREM)
	}

	if n, err := c.DB.SRem(args[0], c.KeyHash, args[1:]...); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}

	return nil
}

func spopCommand(c *Client) error {
	args := c.Args

	if len(args) < 1 || len(args) > 2 {
		return resp.CmdParamsErr(resp.SPOP)
	}

	var count int64 = 1
	if len(args) == 2 {
		var err error
		count, err = utils.ByteToInt64(args[1])
		if err != nil || count < 0 {
			return resp.ErrValue
		}
		if count == 0 {
			c.RespWriter.WriteSliceArray(nil)
			return nil
		}
	}

	res, err := c.DB.SPop(args[0], c.KeyHash, count)
	if err != nil {
		return err
	}
	if len(args) == 2 {
		c.RespWriter.WriteSliceArray(res)
	} else {
		if len(res) >= 1 {
			c.RespWriter.WriteBulk(res[0])
		} else {
			c.RespWriter.WriteBulk(nil)
		}
	}
	return nil
}

func sclearCommand(c *Client) error {
	args := c.Args
	if len(args) < 1 {
		return resp.CmdParamsErr(resp.SCLEAR)
	}

	if n, err := c.DB.SClear(c.KeyHash, args...); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}

	return nil
}

func sexpireCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.SEXPIRE)
	}

	duration, err := utils.ByteToInt64(args[1])
	if err != nil {
		return resp.ErrValue
	}

	var n int64
	n, err = c.DB.Expire(args[0], c.KeyHash, duration)
	if err != nil {
		return err
	}
	c.RespWriter.WriteInteger(n)
	return nil
}

func sexpireAtCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.SEXPIREAT)
	}

	when, err := utils.ByteToInt64(args[1])
	if err != nil {
		return resp.ErrValue
	}

	var n int64
	n, err = c.DB.ExpireAt(args[0], c.KeyHash, when)
	if err != nil {
		return err
	}
	c.RespWriter.WriteInteger(n)
	return nil
}

func sttlCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.STTL)
	}

	if v, err := c.DB.TTl(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(v)
	}

	return nil

}

func spersistCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.SPERSIST)
	}

	if n, err := c.DB.Persist(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}

	return nil
}

func skeyexistsCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.SKEYEXISTS)
	}

	if n, err := c.DB.Exists(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}
	return nil
}
