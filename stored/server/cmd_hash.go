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
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

func init() {
	AddCommand(map[string]*Cmd{
		resp.HDEL:    {Sync: resp.IsWriteCmd(resp.HDEL), Handler: hdelCommand},
		resp.HINCRBY: {Sync: resp.IsWriteCmd(resp.HINCRBY), Handler: hincrbyCommand},
		resp.HMSET:   {Sync: resp.IsWriteCmd(resp.HMSET), Handler: hmsetCommand},
		resp.HSET:    {Sync: resp.IsWriteCmd(resp.HSET), Handler: hsetCommand},
		resp.HVALS:   {Sync: resp.IsWriteCmd(resp.HVALS), Handler: hvalsCommand},
		resp.HEXISTS: {Sync: resp.IsWriteCmd(resp.HEXISTS), Handler: hexistsCommand},
		resp.HGET:    {Sync: resp.IsWriteCmd(resp.HGET), Handler: hgetCommand},
		resp.HGETALL: {Sync: resp.IsWriteCmd(resp.HGETALL), Handler: hgetallCommand},
		resp.HKEYS:   {Sync: resp.IsWriteCmd(resp.HKEYS), Handler: hkeysCommand},
		resp.HLEN:    {Sync: resp.IsWriteCmd(resp.HLEN), Handler: hlenCommand},
		resp.HMGET:   {Sync: resp.IsWriteCmd(resp.HMGET), Handler: hmgetCommand},

		resp.HCLEAR:     {Sync: resp.IsWriteCmd(resp.HCLEAR), Handler: hclearCommand, KeySkip: 1},
		resp.HEXPIRE:    {Sync: resp.IsWriteCmd(resp.HEXPIRE), Handler: hexpireCommand},
		resp.HEXPIREAT:  {Sync: resp.IsWriteCmd(resp.HEXPIREAT), Handler: hexpireAtCommand},
		resp.HPERSIST:   {Sync: resp.IsWriteCmd(resp.HPERSIST), Handler: hpersistCommand},
		resp.HKEYEXISTS: {Sync: resp.IsWriteCmd(resp.HKEYEXISTS), Handler: hkeyexistsCommand},
		resp.HTTL:       {Sync: resp.IsWriteCmd(resp.HTTL), Handler: httlCommand},
	})
}

func hsetCommand(c *Client) error {
	args := c.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.HSET)
	}

	if n, err := c.DB.HSet(args[0], c.KeyHash, args[1], args[2]); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}

	return nil
}

func hgetCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.HGET)
	}

	v, vCloser, err := c.DB.HGet(args[0], c.KeyHash, args[1])
	defer func() {
		if vCloser != nil {
			vCloser()
		}
	}()
	if err != nil {
		return err
	}

	c.RespWriter.WriteBulk(v)
	return nil
}

func hexistsCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.HEXISTS)
	}

	var n int64 = 1

	v, vCloser, err := c.DB.HGet(args[0], c.KeyHash, args[1])
	defer func() {
		if vCloser != nil {
			vCloser()
		}
	}()
	if err != nil {
		return err
	} else {
		if v == nil {
			n = 0
		}

		c.RespWriter.WriteInteger(n)
	}
	return nil
}

func hdelCommand(c *Client) error {
	args := c.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.HDEL)
	}

	if n, err := c.DB.HDel(args[0], c.KeyHash, args[1:]...); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}

	return nil
}

func hlenCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.HLEN)
	}

	if n, err := c.DB.HLen(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}

	return nil
}

func hincrbyCommand(c *Client) error {
	args := c.Args
	if len(args) != 3 {
		return resp.CmdParamsErr(resp.HINCRBY)
	}

	delta, err := utils.ByteToInt64(args[2])

	if err != nil {
		return resp.ErrValue
	}

	var n int64

	if n, err = c.DB.HIncrBy(args[0], c.KeyHash, args[1], delta); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}
	return nil
}

func hmsetCommand(c *Client) error {
	args := c.Args
	if len(args) < 3 {
		return resp.CmdParamsErr(resp.HMSET)
	}

	if len(args[1:])%2 != 0 {
		return resp.CmdParamsErr(resp.HMSET)
	}

	key := args[0]

	args = args[1:]

	kvs := make([]btools.FVPair, len(args)/2)
	for i := 0; i < len(kvs); i++ {
		kvs[i].Field = args[2*i]
		kvs[i].Value = args[2*i+1]
	}

	if err := c.DB.HMset(key, c.KeyHash, kvs...); err != nil {
		return err
	} else {
		c.RespWriter.WriteStatus(resp.ReplyOK)
	}

	return nil
}

func hmgetCommand(c *Client) error {
	args := c.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.HMGET)
	}

	v, vClosers, err := c.DB.HMget(args[0], c.KeyHash, args[1:]...)
	defer func() {
		if len(vClosers) > 0 {
			for _, vCloser := range vClosers {
				vCloser()
			}
		}
	}()
	if err != nil {
		return err
	}

	c.RespWriter.WriteSliceArray(v)
	return nil
}

func hgetallCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.HGETALL)
	}

	v, closers, err := c.DB.HGetAll(args[0], c.KeyHash)
	defer func() {
		if len(closers) > 0 {
			for _, closer := range closers {
				closer()
			}
		}
	}()
	if err != nil {
		return err
	}

	c.RespWriter.WriteFVPairArray(v)
	return nil
}

func hkeysCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.HKEYS)
	}

	v, closers, err := c.DB.HKeys(args[0], c.KeyHash)
	defer func() {
		if len(closers) > 0 {
			for _, closer := range closers {
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

func hvalsCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.HVALS)
	}

	v, closers, err := c.DB.HValues(args[0], c.KeyHash)
	defer func() {
		if len(closers) > 0 {
			for _, closer := range closers {
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

func hclearCommand(c *Client) error {
	args := c.Args
	if len(args) < 1 {
		return resp.CmdParamsErr(resp.HCLEAR)
	}

	if n, err := c.DB.HClear(c.KeyHash, args...); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}

	return nil
}

func hexpireCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.HEXPIRE)
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

func hexpireAtCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.HEXPIREAT)
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

func httlCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.HTTL)
	}

	if v, err := c.DB.TTl(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(v)
	}

	return nil
}

func hpersistCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.HPERSIST)
	}

	if n, err := c.DB.Persist(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}

	return nil
}

func hkeyexistsCommand(c *Client) error {
	args := c.Args
	if len(args) != 1 {
		return resp.CmdParamsErr(resp.HKEYEXISTS)
	}

	if n, err := c.DB.Exists(args[0], c.KeyHash); err != nil {
		return err
	} else {
		c.RespWriter.WriteInteger(n)
	}
	return nil
}
