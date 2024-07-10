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
	"bytes"

	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

func init() {
	AddCommand(map[string]*Cmd{
		resp.LPOP:       {Sync: resp.IsWriteCmd(resp.LPOP), Handler: lpopCommand},
		resp.LPUSH:      {Sync: resp.IsWriteCmd(resp.LPUSH), Handler: lpushCommand},
		resp.RPOP:       {Sync: resp.IsWriteCmd(resp.RPOP), Handler: rpopCommand},
		resp.RPUSH:      {Sync: resp.IsWriteCmd(resp.RPUSH), Handler: rpushCommand},
		resp.LINDEX:     {Sync: resp.IsWriteCmd(resp.LINDEX), Handler: lindexCommand},
		resp.LLEN:       {Sync: resp.IsWriteCmd(resp.LLEN), Handler: llenCommand},
		resp.LRANGE:     {Sync: resp.IsWriteCmd(resp.LRANGE), Handler: lrangeCommand},
		resp.LTRIM:      {Sync: resp.IsWriteCmd(resp.LTRIM), Handler: lTrimCommand},
		resp.LREM:       {Sync: resp.IsWriteCmd(resp.LREM), Handler: lremCommand},
		resp.LINSERT:    {Sync: resp.IsWriteCmd(resp.LINSERT), Handler: linsertCommand},
		resp.LPUSHX:     {Sync: resp.IsWriteCmd(resp.LPUSHX), Handler: lpushxCommand},
		resp.RPUSHX:     {Sync: resp.IsWriteCmd(resp.RPUSHX), Handler: rpushxCommand},
		resp.LSET:       {Sync: resp.IsWriteCmd(resp.LSET), Handler: lsetCommand},
		resp.LCLEAR:     {Sync: resp.IsWriteCmd(resp.LCLEAR), Handler: lclearCommand, KeySkip: 1},
		resp.LPERSIST:   {Sync: resp.IsWriteCmd(resp.LPERSIST), Handler: lpersistCommand},
		resp.LEXPIRE:    {Sync: resp.IsWriteCmd(resp.LEXPIRE), Handler: lexpireCommand},
		resp.LEXPIREAT:  {Sync: resp.IsWriteCmd(resp.LEXPIREAT), Handler: lexpireAtCommand},
		resp.LTRIMFRONT: {Sync: resp.IsWriteCmd(resp.LTRIMFRONT), Handler: lTrimFrontCommand},
		resp.LTRIMBACK:  {Sync: resp.IsWriteCmd(resp.LTRIMBACK), Handler: lTrimBackCommand},
		resp.LTTL:       {Sync: resp.IsWriteCmd(resp.LTTL), Handler: lttlCommand},
		resp.LKEYEXISTS: {Sync: resp.IsWriteCmd(resp.LKEYEXISTS), Handler: lkeyexistsCommand},
	})
}

func lremCommand(c *Client) error {
	args := c.Args
	if len(args) != 3 {
		return errn.CmdParamsErr(resp.LREM)
	}

	count, err := utils.ByteToInt64(args[1])
	if err != nil {
		return errn.ErrValue
	}

	if n, err := c.DB.LRem(args[0], c.KeyHash, count, args[2]); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}

	return nil
}

func linsertCommand(c *Client) error {
	args := c.Args
	if len(args) != 4 {
		return errn.CmdParamsErr(resp.LINSERT)
	}
	isbefore := false
	if bytes.Equal(LowerSlice(args[1]), BEFORE) {
		isbefore = true
	} else if bytes.Equal(LowerSlice(args[1]), AFTER) {
		isbefore = false
	} else {
		return errn.ErrSyntax
	}

	if n, err := c.DB.LInsert(args[0], c.KeyHash, isbefore, args[2], args[3]); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}
	return nil
}

func lpushCommand(c *Client) error {
	args := c.Args
	if len(args) < 2 {
		return errn.CmdParamsErr(resp.LPUSH)
	}

	if n, err := c.DB.LPush(args[0], c.KeyHash, args[1:]...); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}
	return nil
}

func lpushxCommand(c *Client) error {
	args := c.Args
	if len(args) < 2 {
		return errn.CmdParamsErr(resp.LPUSHX)
	}

	if n, err := c.DB.LPushX(args[0], c.KeyHash, args[1:]...); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}

	return nil
}

func rpushCommand(c *Client) error {
	args := c.Args
	if len(args) < 2 {
		return errn.CmdParamsErr(resp.RPUSH)
	}

	if n, err := c.DB.RPush(args[0], c.KeyHash, args[1:]...); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}

	return nil
}

func rpushxCommand(c *Client) error {
	args := c.Args
	if len(args) < 2 {
		return errn.CmdParamsErr(resp.RPUSHX)
	}

	if n, err := c.DB.RPushX(args[0], c.KeyHash, args[1:]...); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}

	return nil
}

func lpopCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return errn.CmdParamsErr(resp.LPOP)
	}

	v, vcloser, err := c.DB.LPop(args[0], c.KeyHash)
	defer func() {
		if vcloser != nil {
			vcloser()
		}
	}()
	if err != nil {
		return err
	}

	c.Writer.WriteBulk(v)
	return nil
}

func rpopCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return errn.CmdParamsErr(resp.RPOP)
	}

	v, vcloser, err := c.DB.RPop(args[0], c.KeyHash)
	defer func() {
		if vcloser != nil {
			vcloser()
		}
	}()
	if err != nil {
		return err
	}

	c.Writer.WriteBulk(v)
	return nil
}

func llenCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return errn.CmdParamsErr(resp.LLEN)
	}

	if n, err := c.DB.LLen(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}

	return nil
}

func lindexCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return errn.CmdParamsErr(resp.LINDEX)
	}

	index, err := utils.ByteToInt64(args[1])
	if err != nil {
		return errn.ErrValue
	}

	v, closer, err := c.DB.LIndex(args[0], c.KeyHash, index)
	defer func() {
		if closer != nil {
			closer()
		}
	}()
	if err != nil {
		return err
	}

	c.Writer.WriteBulk(v)
	return nil
}

func lsetCommand(c *Client) error {
	args := c.Args
	if len(args) != 3 {
		return errn.CmdParamsErr(resp.LSET)
	}

	index, err := utils.ByteToInt64(args[1])
	if err != nil {
		return errn.ErrValue
	}

	if err := c.DB.LSet(args[0], c.KeyHash, index, args[2]); err != nil {
		return err
	} else {
		c.Writer.WriteStatus(resp.ReplyOK)
	}

	return nil
}

func lrangeCommand(c *Client) error {
	args := c.Args
	if len(args) != 3 {
		return errn.CmdParamsErr(resp.LRANGE)
	}

	var start int64
	var stop int64
	var err error

	start, err = utils.ByteToInt64(args[1])
	if err != nil {
		return errn.ErrValue
	}

	stop, err = utils.ByteToInt64(args[2])
	if err != nil {
		return errn.ErrValue
	}

	if v, err := c.DB.LRange(args[0], c.KeyHash, start, stop); err != nil {
		return err
	} else {
		c.Writer.WriteSliceArray(v)
	}

	return nil
}

func lclearCommand(c *Client) error {
	args := c.Args
	if len(args) < 1 {
		return errn.CmdParamsErr(resp.LCLEAR)
	}

	if n, err := c.DB.LClear(c.KeyHash, args...); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}
	return nil
}

func lexpireCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return errn.CmdParamsErr(resp.LEXPIRE)
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

func lexpireAtCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return errn.CmdParamsErr(resp.LEXPIREAT)
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

func lttlCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return errn.CmdParamsErr(resp.LTTL)
	}

	if v, err := c.DB.TTl(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(v)
	}

	return nil
}

func lpersistCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return errn.CmdParamsErr(resp.LPERSIST)
	}

	if n, err := c.DB.Persist(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}

	return nil
}

func lkeyexistsCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return errn.CmdParamsErr(resp.LKEYEXISTS)
	}

	if n, err := c.DB.Exists(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}
	return nil
}

func lTrimCommand(c *Client) error {
	args := c.Args
	if len(args) != 3 {
		return errn.CmdParamsErr(resp.LTRIM)
	}

	var start int64
	var stop int64
	var err error

	start, err = utils.ByteToInt64(args[1])
	if err != nil {
		return errn.ErrValue
	}
	stop, err = utils.ByteToInt64(args[2])
	if err != nil {
		return errn.ErrValue
	}

	if err := c.DB.LTrim(args[0], c.KeyHash, start, stop); err != nil {
		return err
	} else {
		c.Writer.WriteStatus(resp.ReplyOK)
	}

	return nil
}

func lTrimFrontCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return errn.CmdParamsErr(resp.LTRIMFRONT)
	}

	trimSize, err := utils.ByteToInt64(args[1])
	if err != nil || trimSize < 0 {
		return errn.ErrValue
	}

	if n, err := c.DB.LTrimFront(args[0], c.KeyHash, trimSize); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}

	return nil
}

func lTrimBackCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return errn.CmdParamsErr(resp.LTRIMBACK)
	}

	trimSize, err := utils.ByteToInt64(args[1])
	if err != nil || trimSize < 0 {
		return errn.ErrValue
	}

	if n, err := c.DB.LTrimBack(args[0], c.KeyHash, trimSize); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(int64(n))
	}

	return nil
}
