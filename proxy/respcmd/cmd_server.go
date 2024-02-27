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
	"os"
	"syscall"

	"github.com/zuoyebang/bitalostored/proxy/resp"
)

func init() {
	resp.Register(resp.INFO, InfoCommand)
	resp.Register(resp.COMMAND, CommandCommand)
	resp.Register(resp.PING, PingCommand)
	resp.Register(resp.ECHO, EchoCommand)
	resp.Register(resp.SHUTDOWN, ShutdownCommand)
	resp.Register(resp.AUTH, AuthCommand)
}

func InfoCommand(s *resp.Session) error {
	status := "status: true"
	s.RespWriter.WriteStatus(status)
	return nil
}

func PingCommand(s *resp.Session) error {
	if len(s.Args) > 1 {
		return resp.CmdParamsErr(resp.PING)
	}
	if len(s.Args) == 0 {
		s.RespWriter.WriteStatus(resp.ReplyPONG)
	} else {
		s.RespWriter.WriteBulk(s.Args[0])
	}
	return nil
}

func EchoCommand(s *resp.Session) error {
	if len(s.Args) != 1 {
		return resp.CmdParamsErr(resp.ECHO)
	}

	s.RespWriter.WriteBulk(s.Args[0])
	return nil
}

func CommandCommand(s *resp.Session) error {
	s.RespWriter.WriteStatus(resp.ReplyOK)
	return nil
}

func ShutdownCommand(s *resp.Session) error {
	if !s.IsAdmin() {
		return resp.NotFoundErr
	}
	s.Close()

	p, _ := os.FindProcess(os.Getpid())
	p.Signal(syscall.SIGTERM)
	p.Signal(os.Interrupt)
	return nil
}

func AuthCommand(s *resp.Session) error {
	if err := s.DoAuth(); err != nil {
		return err
	}
	s.RespWriter.WriteStatus(resp.ReplyOK)
	return nil
}
