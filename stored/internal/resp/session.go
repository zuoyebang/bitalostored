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

package resp

import (
	"net"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalostored/stored/internal/dostats"
	"github.com/zuoyebang/bitalostored/stored/plugin/anticc"

	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

type Session struct {
	Cmd        string
	Args       [][]byte
	Keys       []byte
	Data       [][]byte
	ID         int
	RespWriter *RespWriter
	RespReader *RespReader

	conn          net.Conn
	remoteAddr    string
	keepalive     time.Duration
	isAuthed      bool
	isRaftSession bool
}

var (
	SessionId      int32 = 0
	ConnBufferSize int   = 8 << 10
)

func NewSession(conn net.Conn, keepalive time.Duration) *Session {
	var s *Session

	if conn == nil {
		s = &Session{
			RespWriter:    NewRespWriter(ConnBufferSize),
			isRaftSession: true,
		}
		s.ID = int(atomic.AddInt32(&SessionId, 1))
	} else {
		s = &Session{
			conn:          conn,
			Cmd:           "",
			Args:          nil,
			keepalive:     keepalive,
			isAuthed:      false,
			RespReader:    NewRespReader(conn, ConnBufferSize),
			RespWriter:    NewRespWriter(ConnBufferSize),
			isRaftSession: false,
		}
		s.ID = int(atomic.AddInt32(&SessionId, 1))
	}
	s.doStats()
	return s
}

func (s *Session) SetReadDeadline() {
	if anticc.Enable && !s.isRaftSession {
		s.conn.SetReadDeadline(anticc.GetConfigDeadline())
	} else {
		if s.keepalive > 0 {
			s.conn.SetReadDeadline(time.Now().Add(s.keepalive))
		}
	}
}

func (s *Session) Close() {
	if s.conn == nil {
		return
	}

	if err := s.conn.Close(); err != nil {
		log.Errorf("conn close err:%v", err)
		return
	}

	if !s.isRaftSession {
		dostats.DecrConns()
	}
}

func (s *Session) doStats() {
	if s.isRaftSession {
		return
	}
	dostats.IncrConns()
}
