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
	"github.com/zuoyebang/bitalostored/stored/engine/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
)

func init() {
	AddCommand(map[string]*Cmd{
		resp.DKCREATE:      {Sync: resp.IsWriteCmd(resp.DKCREATE), Handler: dkcreateCommand},
		resp.DKCREATESHARD: {Sync: resp.IsWriteCmd(resp.DKCREATESHARD), Handler: dkcreateshardCommand},
		resp.DKINFO:        {Sync: resp.IsWriteCmd(resp.DKINFO), Handler: dkinfoCommand},
		resp.DKINCRBYSIZE:  {Sync: resp.IsWriteCmd(resp.DKINCRBYSIZE), Handler: dkincrbysizeCommand},
		resp.DKHSET:        {Sync: resp.IsWriteCmd(resp.DKHSET), Handler: dkhsetCommand},
		resp.DKHDEL:        {Sync: resp.IsWriteCmd(resp.DKHDEL), Handler: dkhdelCommand},
		resp.DKHGET:        {Sync: resp.IsWriteCmd(resp.DKHGET), Handler: dkhgetCommand},
		resp.DKHMGET:       {Sync: resp.IsWriteCmd(resp.DKHMGET), Handler: dkhmgetCommand},
		resp.DKSADD:        {Sync: resp.IsWriteCmd(resp.DKSADD), Handler: dksaddCommand},
		resp.DKSPOP:        {Sync: resp.IsWriteCmd(resp.DKSPOP), Handler: dkspopCommand},
		resp.DKSREM:        {Sync: resp.IsWriteCmd(resp.DKSREM), Handler: dksremCommand},
	})
}

func dkcreateCommand(c *Client) error {
	args := c.Args
	if len(args) != 3 {
		return errn.CmdParamsErr(resp.DKCREATE)
	}

	shardNum, err := utils.ByteToUint32(args[1])
	if err != nil {
		return errn.ErrValue
	}

	dataType := btools.StringToDataType(unsafe2.String(utils.LowerSlice(args[2])))
	if err = c.DB.DKCreate(args[0], c.KeyHash, dataType, shardNum); err != nil {
		return err
	}

	c.Writer.WriteStatus(resp.ReplyOK)
	return nil
}

func dkcreateshardCommand(c *Client) error {
	args := c.Args
	if len(args) < 2 {
		return errn.CmdParamsErr(resp.DKCREATESHARD)
	}

	dataType := btools.StringToDataType(unsafe2.String(utils.LowerSlice(args[0])))
	if err := c.DB.DKCreateShard(dataType, args[1:]...); err != nil {
		return err
	}

	c.Writer.WriteStatus(resp.ReplyOK)
	return nil
}

func dkinfoCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return errn.CmdParamsErr(resp.DKINFO)
	}

	dataType, shardNum, size, timestamp := c.DB.DKInfo(args[0], c.KeyHash)
	c.Writer.WriteArray([]interface{}{
		extend.FormatUint8ToSlice(dataType),
		extend.FormatUint32ToSlice(shardNum),
		extend.FormatUint64ToSlice(size),
		extend.FormatInt64ToSlice(timestamp),
	})
	return nil
}

func dkincrbysizeCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return errn.CmdParamsErr(resp.DKINCRBYSIZE)
	}

	increment, err := utils.ByteToInt64(args[1])
	if err != nil {
		return errn.ErrValue
	}

	var n int64
	n, err = c.DB.DKIncrBySize(args[0], c.KeyHash, increment)
	if err != nil {
		return err
	}

	c.Writer.WriteInteger(n)
	return nil
}

func dkhsetCommand(c *Client) error {
	args := c.Args
	argsNum := len(args)
	if argsNum < 4 {
		return errn.CmdParamsErr(resp.DKHSET)
	}

	res, err := c.DB.DKHSet(args...)
	if err != nil {
		return err
	}

	c.Writer.WriteSliceArray(res)
	return nil
}

func dkhdelCommand(c *Client) error {
	args := c.Args
	argsNum := len(args)
	if argsNum < 3 {
		return errn.CmdParamsErr(resp.DKHDEL)
	}

	res, err := c.DB.DKHDel(args...)
	if err != nil {
		return err
	}

	c.Writer.WriteSliceArray(res)
	return nil
}

func dkhgetCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return errn.CmdParamsErr(resp.DKHGET)
	}

	v, closer, err := c.DB.HGet(args[0], c.KeyHash, args[1])
	if err != nil {
		return err
	}
	defer func() {
		if closer != nil {
			closer()
		}
	}()

	c.Writer.WriteBulk(v)
	return nil
}

func dkhmgetCommand(c *Client) error {
	args := c.Args
	argsNum := len(args)
	if argsNum < 3 {
		return errn.CmdParamsErr(resp.DKHMGET)
	}

	res, closers, err := c.DB.DKHMGet(args...)
	if err != nil {
		return err
	}
	defer func() {
		for _, closer := range closers {
			if closer != nil {
				closer()
			}
		}
	}()
	c.Writer.WriteSliceArray(res)
	return nil
}

func dksaddCommand(c *Client) error {
	args := c.Args
	argsNum := len(args)
	if argsNum < 3 {
		return errn.CmdParamsErr(resp.DKSADD)
	}

	res, err := c.DB.DKSAdd(args...)
	if err != nil {
		return err
	}

	c.Writer.WriteSliceArray(res)
	return nil
}

func dkspopCommand(c *Client) error {
	args := c.Args

	if len(args) < 1 || len(args) > 2 {
		return errn.CmdParamsErr(resp.SPOP)
	} else if err := btools.CheckKeySize(args[0]); err != nil {
		return err
	}

	var count int64 = 1
	if len(args) == 2 {
		var err error
		count, err = utils.ByteToInt64(args[1])
		if err != nil || count < 0 {
			return errn.ErrValue
		} else if count == 0 {
			c.Writer.WriteSliceArray(nil)
			return nil
		}
	}

	res, closer, err := c.DB.DKSPop(args[0], c.KeyHash, count)
	if err != nil {
		return err
	}
	defer func() {
		if closer != nil {
			closer()
		}
	}()

	if len(args) == 2 {
		c.Writer.WriteSliceArray(res)
	} else if len(res) >= 1 {
		c.Writer.WriteBulk(res[0])
	} else {
		c.Writer.WriteBulk(nil)
	}
	return nil
}

func dksremCommand(c *Client) error {
	args := c.Args
	argsNum := len(args)
	if argsNum < 3 {
		return errn.CmdParamsErr(resp.DKSREM)
	}

	res, err := c.DB.DKSRem(args...)
	if err != nil {
		return err
	}

	c.Writer.WriteSliceArray(res)
	return nil
}
