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
	"syscall"

	"github.com/zuoyebang/bitalostored/stored/internal/errn"
)

func init() {
	AddCommand(map[string]*Cmd{
		"shutdown": {Name: "shoutdown the process",
			Handler: func(c *Client) error {
				c.conn.Close()

				p, _ := os.FindProcess(os.Getpid())

				p.Signal(syscall.SIGTERM)
				p.Signal(os.Interrupt)

				return errn.ErrClientQuit
			}},
	})
}
