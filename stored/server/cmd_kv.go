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

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

func init() {
	AddCommand(map[string]*Cmd{
		resp.SET:         {Sync: resp.IsWriteCmd(resp.SET), Handler: setCommand},
		resp.APPEND:      {Sync: resp.IsWriteCmd(resp.APPEND), Handler: appendCommand},
		resp.DECR:        {Sync: resp.IsWriteCmd(resp.DECR), Handler: decrCommand},
		resp.DECRBY:      {Sync: resp.IsWriteCmd(resp.DECRBY), Handler: decrbyCommand},
		resp.GETSET:      {Sync: resp.IsWriteCmd(resp.GETSET), Handler: getsetCommand},
		resp.INCR:        {Sync: resp.IsWriteCmd(resp.INCR), Handler: incrCommand},
		resp.INCRBY:      {Sync: resp.IsWriteCmd(resp.INCRBY), Handler: incrbyCommand},
		resp.INCRBYFLOAT: {Sync: resp.IsWriteCmd(resp.INCRBYFLOAT), Handler: incrbyfloatCommand},
		resp.MSET:        {Sync: resp.IsWriteCmd(resp.MSET), Handler: msetCommand, KeySkip: 2},
		resp.SETNX:       {Sync: resp.IsWriteCmd(resp.SETNX), Handler: setnxCommand},
		resp.SETEX:       {Sync: resp.IsWriteCmd(resp.SETEX), Handler: setexCommand},
		resp.PSETEX:      {Sync: resp.IsWriteCmd(resp.PSETEX), Handler: psetexCommand},
		resp.SETRANGE:    {Sync: resp.IsWriteCmd(resp.SETRANGE), Handler: setrangeCommand},
		resp.GETRANGE:    {Sync: resp.IsWriteCmd(resp.GETRANGE), Handler: getrangeCommand},
		resp.MGET:        {Sync: resp.IsWriteCmd(resp.MGET), Handler: mgetCommand, KeySkip: 1},
		resp.STRLEN:      {Sync: resp.IsWriteCmd(resp.STRLEN), Handler: strlenCommand},
		resp.GET:         {Sync: resp.IsWriteCmd(resp.GET), Handler: getCommand},
		resp.BITCOUNT:    {Sync: resp.IsWriteCmd(resp.BITCOUNT), Handler: bitcountCommand},
		resp.BITPOS:      {Sync: resp.IsWriteCmd(resp.BITPOS), Handler: bitposCommand},
		resp.GETBIT:      {Sync: resp.IsWriteCmd(resp.GETBIT), Handler: getbitCommand},
		resp.SETBIT:      {Sync: resp.IsWriteCmd(resp.SETBIT), Handler: setbitCommand},

		resp.KDEL:      {Sync: resp.IsWriteCmd(resp.KDEL), Handler: kdelCommand, KeySkip: 1},
		resp.KTTL:      {Sync: resp.IsWriteCmd(resp.KTTL), Handler: kttlCommand},
		resp.KEXISTS:   {Sync: resp.IsWriteCmd(resp.KEXISTS), Handler: kexistsCommand},
		resp.KEXPIRE:   {Sync: resp.IsWriteCmd(resp.KEXPIRE), Handler: kexpireCommand},
		resp.KEXPIREAT: {Sync: resp.IsWriteCmd(resp.KEXPIREAT), Handler: kexpireAtCommand},
		resp.KPERSIST:  {Sync: resp.IsWriteCmd(resp.KPERSIST), Handler: kpersistCommand},
	})
}

func getCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.GET)
	}

	v, closer, err := c.DB.Get(args[0], c.KeyHash)
	defer func() {
		if closer != nil {
			closer()
		}
	}()
	if err != nil {
		return err
	}

	c.RespWriter.WriteBulk(v)
	return nil
}

func setCommand(c *Client) error {
	args := c.Args

	if len(c.Args) < 2 {
		return resp.CmdParamsErr(resp.SET)
	}

	exType, sec, setCondition, err := resp.ParseSetArgs(args[2:])

	if err != nil {
		return err
	}

	if exType == resp.NO_TYPE && setCondition == resp.NO_CONDITION {
		if err := c.DB.Set(args[0], c.KeyHash, args[1]); err != nil {
			return err
		}
		c.RespWriter.WriteStatus(resp.ReplyOK)
	} else if exType == resp.NO_TYPE && setCondition == resp.NX {
		if n, err := c.DB.SetNX(args[0], c.KeyHash, args[1]); err != nil {
			return err
		} else if n == 1 {
			c.RespWriter.WriteStatus(resp.ReplyOK)
		} else {
			c.RespWriter.WriteBulk(nil)
		}
	} else if exType == resp.EX && setCondition == resp.NO_CONDITION {
		if err := c.DB.SetEX(args[0], c.KeyHash, sec, args[1]); err != nil {
			return err
		} else {
			c.RespWriter.WriteStatus(resp.ReplyOK)
		}
	} else if exType == resp.EX && setCondition == resp.NX {
		if n, err := c.DB.SetNXEX(args[0], c.KeyHash, sec, args[1]); err != nil {
			return err
		} else if n == 1 {
			c.RespWriter.WriteStatus(resp.ReplyOK)
		} else {
			c.RespWriter.WriteBulk(nil)
		}
	} else if exType == resp.PX && setCondition == resp.NO_CONDITION {
		if err := c.DB.PSetEX(args[0], c.KeyHash, sec, args[1]); err != nil {
			return err
		} else {
			c.RespWriter.WriteStatus(resp.ReplyOK)
		}
	} else if exType == resp.PX && setCondition == resp.NX {
		if n, err := c.DB.PSetNXEX(args[0], c.KeyHash, sec, args[1]); err != nil {
			return err
		} else if n == 1 {
			c.RespWriter.WriteStatus(resp.ReplyOK)
		} else {
			c.RespWriter.WriteBulk(nil)
		}
	} else {
		return resp.ErrNotImplement
	}

	return nil
}

func getsetCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.GETSET)
	}

	v, closer, err := c.DB.GetSet(args[0], c.KeyHash, args[1])
	defer func() {
		if closer != nil {
			closer()
		}
	}()
	if err != nil {
		return err
	}

	c.RespWriter.WriteBulk(v)
	return nil
}

func setnxCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.SETNX)
	}

	if n, err := c.DB.SetNX(args[0], c.KeyHash, args[1]); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}
	return nil
}

func setexCommand(c *Client) error {
	args := c.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.SETEX)
	}

	sec, err := utils.ByteToInt64(args[1])
	if err != nil {
		return resp.ErrValue
	}

	if err := c.DB.SetEX(args[0], c.KeyHash, sec, args[2]); err != nil {
		return err
	} else {
		c.RespWriter.WriteStatus(resp.ReplyOK)
	}

	return nil
}

func psetexCommand(c *Client) error {
	args := c.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.SETEX)
	}

	mills, err := utils.ByteToInt64(args[1])
	if err != nil {
		return resp.ErrValue
	}

	if err := c.DB.PSetEX(args[0], c.KeyHash, mills, args[2]); err != nil {
		return err
	} else {
		c.RespWriter.WriteStatus(resp.ReplyOK)
	}

	return nil
}

func kexistsCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.EXISTS)
	}

	if n, err := c.DB.Exists(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}

	return nil
}

func incrCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.INCR)
	}

	if n, err := c.DB.Incr(c.Args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}

	return nil
}

func decrCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.DECR)
	}

	if n, err := c.DB.Decr(c.Args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}

	return nil
}

func incrbyCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.INCRBY)
	}

	delta, err := utils.ByteToInt64(args[1])
	if err != nil {
		return resp.ErrValue
	}

	if n, err := c.DB.IncrBy(c.Args[0], c.KeyHash, delta); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}

	return nil
}

func incrbyfloatCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.INCRBYFLOAT)
	}
	delta, err := utils.ByteToFloat64(args[1])
	if err != nil {
		return resp.ErrValue
	}

	if n, err := c.DB.IncrByFloat(c.Args[0], c.KeyHash, delta); err != nil {
		return err
	} else {
		c.RespWriter.WriteBulk([]byte(strconv.FormatFloat(n, 'f', -1, 64)))
	}

	return nil
}

func decrbyCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.DECRBY)
	}

	delta, err := utils.ByteToInt64(args[1])
	if err != nil {
		return resp.ErrValue
	}

	if n, err := c.DB.DecrBy(c.Args[0], c.KeyHash, delta); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}

	return nil
}

func kdelCommand(c *Client) error {
	args := c.Args
	if len(args) == 0 {
		return resp.CmdParamsErr(resp.KDEL)
	}

	if n, err := c.DB.Del(c.KeyHash, args...); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}

	return nil
}

func msetCommand(c *Client) error {
	args := c.Args
	if len(args) == 0 || len(args)%2 != 0 {
		return resp.CmdParamsErr(resp.MSET)
	}

	kvs := make([]btools.KVPair, len(args)/2)
	for i := 0; i < len(kvs); i++ {
		kvs[i].Key = args[2*i]
		kvs[i].Value = args[2*i+1]
	}

	if err := c.DB.MSet(c.KeyHash, kvs...); err != nil {
		return err
	} else {
		c.RespWriter.WriteStatus(resp.ReplyOK)
	}

	return nil
}

func mgetCommand(c *Client) error {
	args := c.Args
	if len(args) == 0 {
		return resp.CmdParamsErr(resp.MGET)
	}

	v, closers, err := c.DB.MGet(c.KeyHash, args...)
	defer func() {
		for _, closer := range closers {
			if closer != nil {
				closer()
			}
		}
	}()
	if err != nil {
		return err
	}

	c.RespWriter.WriteSliceArray(v)
	return nil
}

func kexpireCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.KEXPIRE)
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

func kexpireAtCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.KEXPIREAT)
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

func kttlCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.KTTL)
	}

	if v, err := c.DB.TTl(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(v)
	}

	return nil
}

func kpersistCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.PERSIST)
	}

	if n, err := c.DB.Persist(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}

	return nil
}

func appendCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.APPEND)
	}

	if n, err := c.DB.Append(args[0], c.KeyHash, args[1]); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}
	return nil
}

func getrangeCommand(c *Client) error {
	args := c.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.GETRANGE)
	}

	key := args[0]
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}

	end, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return err
	}

	v, closer, err := c.DB.GetRange(key, c.KeyHash, start, end)
	defer func() {
		if closer != nil {
			closer()
		}
	}()
	if err != nil {
		return err
	} else {
		c.RespWriter.WriteBulk(v)
	}

	return nil

}

func setrangeCommand(c *Client) error {
	args := c.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.SETRANGE)
	}

	key := args[0]
	offset, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}
	if offset < 0 {
		return resp.ErrRangeOffset
	}

	value := args[2]

	if n, err := c.DB.SetRange(key, c.KeyHash, offset, value); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}
	return nil
}

func strlenCommand(c *Client) error {
	if len(c.Args) != 1 {
		return resp.CmdParamsErr(resp.STRLEN)
	}

	if n, err := c.DB.StrLen(c.Args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}
	return nil
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
		return resp.CmdParamsErr(resp.BITCOUNT)
	}

	key := args[0]
	start, end, err := parseBitRange(args[1:])
	if err != nil {
		return err
	}
	if start > end && len(args[1:]) != 0 {
		c.RespWriter.WriteInteger(0)
		return nil
	}

	if n, err := c.DB.BitCount(key, c.KeyHash, start, end); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}
	return nil
}

func bitposCommand(c *Client) error {
	args := c.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.BITPOS)
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
		c.RespWriter.WriteInteger(n)
	}
	return nil
}

func getbitCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.GETBIT)
	}

	key := args[0]
	offset, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}
	if offset < 0 {
		return resp.ErrBitOffset
	}

	if n, err := c.DB.GetBit(key, c.KeyHash, offset); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}
	return nil
}

func setbitCommand(c *Client) error {
	args := c.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.SETBIT)
	}

	key := args[0]
	offset, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return err
	}
	if offset < 0 {
		return resp.ErrBitOffset
	}

	value, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return err
	}

	if n, err := c.DB.SetBit(key, c.KeyHash, offset, value); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}
	return nil
}
