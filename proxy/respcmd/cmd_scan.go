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

package respcmd

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/proxy/internal/log"
	"github.com/zuoyebang/bitalostored/proxy/resp"
	"github.com/zuoyebang/bitalostored/proxy/router"
)

func init() {
	resp.Register(resp.HSCAN, scanGroup.HscanCommand)
	resp.Register(resp.SSCAN, scanGroup.SscanCommand)
	resp.Register(resp.ZSCAN, scanGroup.ZscanCommand)
}

func parseXScanArgs(args [][]byte) (cursor []byte, match string, count int, err error) {
	cursor = args[0]
	args = args[1:]
	count = 10

	for i := 0; i < len(args); {
		switch strings.ToUpper(unsafe2.String(args[i])) {
		case "MATCH":
			if i+1 >= len(args) {
				err = resp.SyntaxErr
				return
			}
			match = unsafe2.String(args[i+1])
			i++
		case "COUNT":
			if i+1 >= len(args) {
				err = resp.SyntaxErr
				return
			}
			count, err = strconv.Atoi(unsafe2.String(args[i+1]))
			if err != nil {
				return
			}
			i++
		default:
			err = fmt.Errorf("invalid argument %s", string(args[i]))
			log.Warn("parseXScanArgs err : ", err)
			return
		}
		i++
	}

	return
}

func parseScanArgs(args [][]byte) (cursor []byte, match string, count int, err error) {
	cursor, match, count, err = parseXScanArgs(args)
	return
}

type scanCommandGroup struct {
	lastCursor []byte
	parseArgs  func(args [][]byte) (cursor []byte, match string, count int, err error)
}

func (scg scanCommandGroup) HscanCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.HSCAN)
	}
	key := args[0]

	cursor, match, count, err := scg.parseArgs(args[1:])
	if err != nil {
		return err
	}
	if count <= 0 {
		return resp.SyntaxErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		nextcur, values, err := proxyClient.HScan(s, key, cursor, match, count)
		if err != nil {
			return err
		}
		data := make([]interface{}, 2)

		data[0] = nextcur

		vv := make([][]byte, 0, len(values))
		for i := 0; i < len(values); i = i + 2 {
			vv = append(vv, values[i], values[i+1])
		}
		data[1] = vv

		s.RespWriter.WriteArray(data)
	} else {
		return err
	}

	return nil
}

func (scg scanCommandGroup) SscanCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.SSCAN)
	}
	key := args[0]

	cursor, match, count, err := scg.parseArgs(args[1:])
	log.Infof("cursor:%v, match:%v, count:%v, err :%v", cursor, match, count, err)
	if err != nil {
		return err
	}
	if count <= 0 {
		return resp.SyntaxErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		nextcur, members, err := proxyClient.SScan(s, unsafe2.String(key), cursor, match, count)
		if err != nil {
			return err
		}

		data := make([]interface{}, 2)
		data[0] = nextcur

		vv := make([][]byte, 0, len(members))
		for _, v := range members {
			vv = append(vv, v)
		}
		data[1] = vv

		s.RespWriter.WriteArray(data)
	} else {
		return err
	}

	return nil
}

func (scg scanCommandGroup) ZscanCommand(s *resp.Session) error {
	args := s.Args
	if len(args) < 2 {
		return resp.CmdParamsErr(resp.ZSCAN)
	}
	key := args[0]

	cursor, match, count, err := scg.parseArgs(args[1:])
	if err != nil {
		return err
	}
	if count <= 0 {
		return resp.SyntaxErr
	}
	if proxyClient, err := router.GetProxyClient(); err == nil {
		nextcur, values, err := proxyClient.ZScan(s, unsafe2.String(key), cursor, match, count)
		if err != nil {
			return err
		}

		data := make([]interface{}, 2)
		data[0] = nextcur

		vv := make([][]byte, 0, len(values))

		for i := 0; i < len(values); i = i + 2 {
			vv = append(vv, values[i], values[i+1])
		}
		data[1] = vv

		s.RespWriter.WriteArray(data)
	} else {
		return err
	}

	return nil
}

var (
	scanGroup = scanCommandGroup{nilCursorRedis, parseScanArgs}
)

var (
	nilCursorRedis = []byte("0")
)
