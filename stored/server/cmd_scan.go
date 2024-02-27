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
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"

	"github.com/zuoyebang/bitalostored/butils/extend"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
)

func init() {
	AddCommand(map[string]*Cmd{
		resp.SCAN:   {Sync: resp.IsWriteCmd(resp.SCAN), Handler: scanCommand, NotAllowedInTx: true},
		resp.ZSCAN:  {Sync: resp.IsWriteCmd(resp.ZSCAN), Handler: scanGroup.xzscanCommand, NotAllowedInTx: true},
		resp.SSCAN:  {Sync: resp.IsWriteCmd(resp.SSCAN), Handler: scanGroup.xsscanCommand, NotAllowedInTx: true},
		resp.HSCAN:  {Sync: resp.IsWriteCmd(resp.HSCAN), Handler: scanGroup.xhscanCommand, NotAllowedInTx: true},
		resp.XZSCAN: {Sync: resp.IsWriteCmd(resp.XZSCAN), Handler: xScanGroup.xzscanCommand, NotAllowedInTx: true},
		resp.XSSCAN: {Sync: resp.IsWriteCmd(resp.XSSCAN), Handler: xScanGroup.xsscanCommand, NotAllowedInTx: true},
		resp.XHSCAN: {Sync: resp.IsWriteCmd(resp.XHSCAN), Handler: xScanGroup.xhscanCommand, NotAllowedInTx: true},
	})
}

var (
	xScanGroup = scanCommandGroup{nilCursorBitalos, parseXScanArgs}
	scanGroup  = scanCommandGroup{nilCursorRedis, parseScanArgs}
)

var (
	nilCursorBitalos = []byte("")
	nilCursorRedis   = []byte("0")
)

func parseXScanArgs(args [][]byte) (cursor []byte, match string, count int, err error) {
	cursor = args[0]

	args = args[1:]

	count = 10

	for i := 0; i < len(args); {
		switch strings.ToUpper(unsafe2.String(args[i])) {
		case "MATCH":
			if i+1 >= len(args) {
				err = resp.CmdParamsErr("scan")
				return
			}

			match = unsafe2.String(args[i+1])
			i++
		case "COUNT":
			if i+1 >= len(args) {
				err = resp.CmdParamsErr("scan")
				return
			}

			count, err = strconv.Atoi(unsafe2.String(args[i+1]))
			if err != nil {
				return
			}

			i++
		default:
			err = fmt.Errorf("invalid argument %s", args[i])
			return
		}
		i++
	}

	return
}

func parseScanArgs(args [][]byte) (cursor []byte, match string, count int, err error) {
	cursor, match, count, err = parseXScanArgs(args)
	if bytes.Compare(cursor, nilCursorRedis) == 0 {
		cursor = nilCursorBitalos
	}
	return
}

type scanCommandGroup struct {
	lastCursor []byte
	parseArgs  func(args [][]byte) (cursor []byte, match string, count int, err error)
}

func (scg scanCommandGroup) xhscanCommand(c *Client) error {
	args := c.Args

	if len(args) < 2 {
		return resp.CmdParamsErr(resp.HSCAN)
	}

	key := args[0]

	cursor, match, count, err := scg.parseArgs(args[1:])

	if err != nil {
		return err
	}

	var ay []btools.FVPair

	cursor, ay, err = c.DB.HScan(key, c.KeyHash, cursor, count, match)
	if err != nil {
		return err
	}

	data := make([]interface{}, 2)
	vv := make([][]byte, 0, len(ay)*2)

	for _, v := range ay {
		vv = append(vv, v.Field, v.Value)
	}

	data[0] = cursor
	data[1] = vv

	c.RespWriter.WriteArray(data)
	return nil
}

func (scg scanCommandGroup) xsscanCommand(c *Client) error {
	args := c.Args

	if len(args) < 2 {
		return resp.CmdParamsErr(resp.SSCAN)
	}

	key := args[0]

	cursor, match, count, err := scg.parseArgs(args[1:])

	if err != nil {
		return err
	}

	var ay [][]byte

	cursor, ay, err = c.DB.SScan(key, c.KeyHash, cursor, count, match)

	if err != nil {
		return err
	}

	data := make([]interface{}, 2)
	data[0] = cursor
	data[1] = ay

	c.RespWriter.WriteArray(data)
	return nil
}

func (scg scanCommandGroup) xzscanCommand(c *Client) error {
	args := c.Args

	if len(args) < 2 {
		return resp.CmdParamsErr(resp.ZSCAN)
	}

	key := args[0]

	cursor, match, count, err := scg.parseArgs(args[1:])

	if err != nil {
		return err
	}

	var ay []btools.ScorePair

	cursor, ay, err = c.DB.ZScan(key, c.KeyHash, cursor, count, match)
	if err != nil {
		return err
	}

	var data [2]interface{}
	vv := make([][]byte, 0, len(ay)*2)
	for _, v := range ay {
		vv = append(vv, v.Member, extend.FormatFloat64ToSlice(v.Score))
	}

	data[0] = cursor
	data[1] = vv

	c.RespWriter.WriteArray(data[:])
	return nil
}

func scanCommand(c *Client) error {
	args := c.Args
	if len(args) < 1 {
		return resp.CmdParamsErr(resp.SCAN)
	}

	cursor, match, count, tp, err := parseGScanArgs(args)
	if err != nil {
		return err
	}

	if count < 0 {
		return resp.ErrSyntax
	} else if count > 5000 {
		return errors.New("ERR count more than 5000")
	}

	var cur []byte
	var ks [][]byte

	dataType := btools.StringToDataType(tp)
	cur, ks, err = c.DB.Scan(cursor, count, match, dataType)
	if err != nil {
		return err
	}

	if cur == nil {
		cur = []byte("0")
	}
	c.RespWriter.WriteArray([]interface{}{cur, ks})

	return nil
}

func parseGScanArgs(args [][]byte) (cursor []byte, match string, count int, tp string, err error) {
	cursor = args[0]
	args = args[1:]
	count = 10
	tp = ""
	match = ""

	for i := 0; i < len(args); {
		switch strings.ToUpper(unsafe2.String(args[i])) {
		case "MATCH":
			if i+1 >= len(args) {
				err = resp.CmdParamsErr("scan")
				return
			}

			match = unsafe2.String(args[i+1])
			i++
		case "COUNT":
			if i+1 >= len(args) {
				err = resp.CmdParamsErr("scan")
				return
			}

			count, err = strconv.Atoi(unsafe2.String(args[i+1]))
			if err != nil {
				return
			}

			i++
		case "TYPE":
			if i+1 >= len(args) {
				err = resp.CmdParamsErr("scan")
				return
			}

			tp = unsafe2.String(args[i+1])
			i++
		default:
			err = fmt.Errorf("invalid argument %s", args[i])
			return
		}
		i++
	}

	return
}
