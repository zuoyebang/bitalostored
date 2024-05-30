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
	"errors"

	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

type Proc struct {
	Name string

	Start func(*Server)
	Stop  func(*Server, interface{})

	Connect func(*Server, *Client)
	Disconn func(*Server, *Client, interface{})
	Prepare func(*Client, *Cmd, string) bool
	Handled func(*Client, *Cmd, string)

	DoRaftSync func(*Client, *Cmd, string) error
}

var plugins = []*Proc{}
var raftplugin *Proc

func AddPlugin(p *Proc) {
	if p.Name == "" {
		p.Name = log.FileLine(2, 3)
	}
	plugins = append(plugins, p)
}

func AddRaftPlugin(p *Proc) {
	raftplugin = p
}

func runPluginStart(s *Server) {
	for _, p := range plugins {
		if p.Start != nil {
			p.Start(s)
		}
	}
}
func runPluginStop(s *Server, e interface{}) {
	for _, p := range plugins {
		if p.Stop != nil {
			p.Stop(s, e)
		}
	}
}

func runPluginConnect(s *Server, c *Client) {
	for _, p := range plugins {
		if p.Connect != nil {
			p.Connect(s, c)
		}
	}
}
func runPluginDisconn(s *Server, c *Client, e interface{}) {
	for _, p := range plugins {
		if p.Disconn != nil {
			p.Disconn(s, c, e)
		}
	}
}
func runPluginRaft(c *Client, cmd *Cmd, key string) error {
	if raftplugin == nil || raftplugin.DoRaftSync == nil {
		return errors.New("no raft plugin")
	}
	return raftplugin.DoRaftSync(c, cmd, key)

}

func runPluginHandled(c *Client, cmd *Cmd, key string) {
	for _, p := range plugins {
		if p.Handled != nil {
			p.Handled(c, cmd, key)
		}
	}
}
