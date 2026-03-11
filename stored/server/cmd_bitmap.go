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

	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
)

func init() {
	AddCommand(map[string]*Cmd{
		resp.BITCOUNT:   {Sync: resp.IsWriteCmd(resp.BITCOUNT), Handler: bitcountCommand},
		resp.BITPOS:     {Sync: resp.IsWriteCmd(resp.BITPOS), Handler: bitposCommand},
		resp.GETBIT:     {Sync: resp.IsWriteCmd(resp.GETBIT), Handler: getbitCommand},
		resp.SETBIT:     {Sync: resp.IsWriteCmd(resp.SETBIT), Handler: setbitCommand},
		resp.BITCOUNT64: {Sync: resp.IsWriteCmd(resp.BITCOUNT64), Handler: bitcount64Command},
		resp.BITPOS64:   {Sync: resp.IsWriteCmd(resp.BITPOS64), Handler: bitpos64Command},
		resp.GETBIT64:   {Sync: resp.IsWriteCmd(resp.GETBIT64), Handler: getbit64Command},
		resp.SETBIT64:   {Sync: resp.IsWriteCmd(resp.SETBIT64), Handler: setbit64Command},
	})
}

func parseBitRange(args [][]byte) (start int, end int, err error) {
	start = 0
	end = -1
	if len(args) > 0 {
		if start, err = strconv.Atoi(string(args[0])); err != nil {
			return
		}
	}

	if len(args) == 2 {
		if end, err = strconv.Atoi(string(args[1])); err != nil {
			return
		}
	}
	return
}

func bitcountCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 && len(args) != 3 {
		return errn.CmdParamsErr(resp.BITCOUNT)
	}

	key := args[0]
	start, end, err := parseBitRange(args[1:])
	if err != nil {
		return err
	}

	if n, err := c.DB.BitCount(key, c.KeyHash, start, end); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}
	return nil
}

func bitposCommand(c *Client) error {
	args := c.Args
	if len(args) < 2 {
		return errn.CmdParamsErr(resp.BITPOS)
	}

	key := args[0]
	bit, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}

	start, end, err := parseBitRange(args[2:])
	if err != nil {
		return err
	}

	if n, err := c.DB.BitPos(key, c.KeyHash, bit, start, end); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}
	return nil
}

func getbitCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return errn.CmdParamsErr(resp.GETBIT)
	}

	key := args[0]
	offset, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}
	if offset < 0 {
		return errn.ErrBitOffset
	}

	if n, err := c.DB.GetBit(key, c.KeyHash, offset); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}
	return nil
}

func setbitCommand(c *Client) error {
	args := c.Args
	if len(args) != 3 {
		return errn.CmdParamsErr(resp.SETBIT)
	}

	key := args[0]
	offset, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}
	if offset < 0 {
		return errn.ErrBitOffset
	}

	value, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return err
	}

	if n, err := c.DB.SetBit(key, c.KeyHash, offset, value); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}
	return nil
}

func bitcount64Command(c *Client) error {
	args := c.Args

	if len(args) != 1 && len(args) != 3 {
		return errn.CmdParamsErr(resp.BITCOUNT)
	}

	key := args[0]
	start, end, err := parseBitRange(args[1:])
	if err != nil {
		return err
	}

	if n, err := c.DB.BitCount64(key, c.KeyHash, start, end); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
		return nil
	}
}

func bitpos64Command(c *Client) error {
	args := c.Args
	if len(args) < 2 {
		return errn.CmdParamsErr(resp.BITPOS)
	}

	key := args[0]
	bit, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}
	start, end, err := parseBitRange(args[2:])
	if err != nil {
		return err
	}

	if n, err := c.DB.BitPos64(key, c.KeyHash, bit, start, end); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}
	return nil
}

func getbit64Command(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return errn.CmdParamsErr(resp.GETBIT)
	}

	key := args[0]
	offset, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}
	if offset < 0 {
		return errn.ErrBitOffset
	}

	if n, err := c.DB.GetBit64(key, c.KeyHash, offset); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}
	return nil
}

func setbit64Command(c *Client) error {
	args := c.Args
	if len(args) != 3 {
		return errn.CmdParamsErr(resp.SETBIT)
	}

	key := args[0]
	offset, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}
	if offset < 0 {
		return errn.ErrBitOffset
	}

	value, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return err
	}

	if n, err := c.DB.SetBit64(key, c.KeyHash, offset, value); err != nil {
		return err
	} else {
		c.Writer.WriteInteger(n)
	}
	return nil
}
