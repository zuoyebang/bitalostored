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
	"os"

	"github.com/zuoyebang/bitalostored/stored/engine"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/task"
)

type DB struct {
	*engine.Bitalos

	Info *SInfo
}

func init() {
	AddCommand(map[string]*Cmd{
		"_task": {Name: "_task file", Handler: func(c *Client) error {
			task.Run(c, func(task *task.Task) error {
				log.Info("hello world")
				return nil
			})
			c.RespWriter.WriteStatus("OK")
			return nil
		}},
		"_save": {Name: "_save file", Handler: func(c *Client) error {
			return nil
		}},
		"_load": {Name: "_load file", Handler: func(c *Client) error {
			f, e := os.Open(string(c.Args[0]))
			if e != nil {
				return e
			}
			defer f.Close()

			if e := c.server.RecoverFromSnapshot(f, nil); e != nil {
				return e
			}
			c.RespWriter.WriteStatus("OK")
			return nil
		}},
	})
}
