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
	"strings"

	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
)

const (
	CONFIGGET = "GET"
	CONFIGSET = "SET"
)

func init() {
	AddCommand(map[string]*Cmd{
		resp.CONFIG: {Sync: false, Handler: configCommand, NoKey: true},
	})
}

func configCommand(c *Client) error {
	args := c.Args
	if len(args) != 2 {
		return resp.CmdParamsErr(resp.CONFIG)
	}

	op := strings.ToUpper(unsafe2.String(args[0]))
	if op != CONFIGGET {
		return resp.ErrNotImplement
	}

	configName := strings.ToLower(unsafe2.String(args[1]))
	if configName == "maxmemory" {
		fvPair := btools.FVPair{
			Field: []byte("maxmemory"),
			Value: []byte("268435456"),
		}
		c.RespWriter.WriteFVPairArray([]btools.FVPair{fvPair})
	}

	return nil
}
