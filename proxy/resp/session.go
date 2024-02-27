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
	"strings"
	"sync/atomic"
	"time"

	"github.com/zuoyebang/bitalostored/butils/math2"
	"github.com/zuoyebang/bitalostored/proxy/internal/anticc"
	"github.com/zuoyebang/bitalostored/proxy/internal/dostats"
	"github.com/zuoyebang/bitalostored/proxy/internal/log"

	"github.com/gomodule/redigo/redis"
)

const (
	TxStateNone   int = 0
	TxStateWatch  int = 0x1
	TxStateCancel int = 0x10
)

const (
	DefaultCmdType uint8 = 0
	MgetCmdType    uint8 = 1
	MsetCmdType    uint8 = 2
	DelCmdType     uint8 = 3
)

const (
	TxCommandNumLimit = 100
)

type Session struct {
	conn net.Conn
	Cmd  string
	Args [][]byte

	keepalive time.Duration

	authEnabled   bool
	userPassword  string
	adminPassword string
	isAuthed      bool
	isAdmin       bool

	isDealingQuery atomic.Bool
	lastQueryTime  time.Time

	RespWriter *RespWriter
	RespReader *RespReader

	Stats *dostats.CalDoStats

	activeQuit bool

	OpenDistributedTx bool
	TxState           int
	TxCommandQueued   bool
	Recorder          *TxRecorder
}

type TxRecorder struct {
	CmdNum        int
	MultiOk       bool
	ServerClients map[int]*InternalServerConn
	ServerCmdNum  map[int]int
	TxCommands    []*TxCommandServerMap
}

func (tr *TxRecorder) init(clients map[int]*InternalServerConn) {
	tr.ServerClients = clients
	tr.ServerCmdNum = make(map[int]int, len(clients))
	for gid := range clients {
		tr.ServerCmdNum[gid] = 0
	}
	tr.TxCommands = make([]*TxCommandServerMap, 0, 5)
}

func (tr *TxRecorder) Reset() {
	tr.MultiOk = false
	tr.ServerClients = nil
	tr.ServerCmdNum = nil
	tr.TxCommands = nil
	tr.CmdNum = 0
}

type TxCommandServerMap struct {
	Command       uint8
	KeyNum        int
	GroupId       int
	ServerRespSeq map[int]int
	RespGroupMap  map[int][]int
	Response      interface{}
}

func (tr *TxRecorder) AddCommand(gid int) {
	commandMap := TxCommandServerMap{
		Command: DefaultCmdType,
		KeyNum:  1,
		GroupId: gid,
	}
	commandMap.ServerRespSeq = make(map[int]int, 1)
	commandMap.ServerRespSeq[gid] = tr.ServerCmdNum[gid]
	tr.TxCommands = append(tr.TxCommands, &commandMap)
	tr.ServerCmdNum[gid]++
}

type InternalServerConn struct {
	GroupId  int
	Conn     redis.Conn
	HostPort string
}

func NewSession(conn net.Conn, keepalive time.Duration, connReaderBufferSize int, connWriteBufferSize int, openDistributedTx bool) *Session {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetReadBuffer(connWriteBufferSize)
		tcpConn.SetWriteBuffer(connWriteBufferSize)
	}

	s := &Session{
		conn:              conn,
		Cmd:               "",
		Args:              nil,
		keepalive:         keepalive,
		isAuthed:          false,
		RespReader:        NewRespReader(conn, connReaderBufferSize),
		RespWriter:        NewRespWriter(conn, connWriteBufferSize),
		Stats:             dostats.NewCalDoStats(),
		activeQuit:        false,
		OpenDistributedTx: openDistributedTx,
	}
	if openDistributedTx {
		s.Recorder = &TxRecorder{}
	}

	dostats.IncrConns()
	globalSessionManager.AddSession(s)

	return s
}

func (s *Session) SetReadDeadline() {
	if s.keepalive > 0 {
		s.conn.SetReadDeadline(anticc.GetConfigDeadline())
	}
}

func (s *Session) SetLastQueryTime() {
	s.lastQueryTime = time.Now()
}

func (s *Session) SetQueryProperty(v bool) {
	s.isDealingQuery.Store(v)
}

func (s *Session) closeSpareConn() bool {
	currTime := time.Now().Unix()
	currDeadline := anticc.GetConfigDeadline().Unix()
	if s.isDealingQuery.Load() {
		return false
	}
	if 2*math2.Abs(currTime, currDeadline) < math2.Abs(currTime, s.lastQueryTime.Unix()) {
		if !s.isDealingQuery.Load() {
			s.Close()
			return true
		}
	}
	return false
}

func (s *Session) SetAuth(authEnabled bool, userPassword, adminPassword string) {
	s.isAuthed = false
	s.isAdmin = false
	s.authEnabled = authEnabled
	s.userPassword = userPassword
	s.adminPassword = adminPassword
}

func (s *Session) DoAuth() error {
	if len(s.Args) != 1 {
		return CmdParamsErr(AUTH)
	}
	if s.adminPassword == string(s.Args[0]) {
		s.isAuthed = true
		s.isAdmin = true
		return nil
	}
	if s.userPassword == string(s.Args[0]) {
		s.isAuthed = true
		s.isAdmin = false
		return nil
	}
	return AuthenticationFailureErr
}

func (s *Session) IsAdmin() bool {
	return s.isAdmin
}

func (s *Session) ReleaseTxClients() {
	if !s.OpenDistributedTx {
		return
	}
	for _, c := range s.Recorder.ServerClients {
		c.Conn.Close()
	}
}

func (s *Session) Close() {
	s.ReleaseTxClients()
	s.activeQuit = true

	err := s.conn.Close()
	if err != nil {
		log.Infof("close session err:%v", err)
		return
	}

	dostats.DecrConns()
	s.Stats.FlushOpStats(dostats.CmdServer)
}

func (s *Session) Perform(startUnixNano int64) error {
	var err error
	s.Stats.IncrOpTotal()

	if s.OpenDistributedTx && !s.checkTxCommandNum() {
		s.Recorder.CmdNum++
		s.RespWriter.WriteStatus(ReplyQUEUED)
		return nil
	}

	if len(s.Cmd) == 0 {
		err = EmptyCommandErr
	} else if exeCmd, ok := regCmds[s.Cmd]; !ok {
		err = NotFoundErr
		if s.OpenDistributedTx {
			s.SetTxCancel(err)
		}
	} else if s.authEnabled && !s.isAuthed && s.Cmd != AUTH {
		err = NotAuthenticatedErr
	} else {
		err = exeCmd(s)
		if s.OpenDistributedTx {
			s.SetTxCancel(err)
		}
	}

	if err != nil {
		if err == redis.ErrNil {
			s.RespWriter.WriteBulk(nil)
			err = nil
		} else {
			s.Stats.IncrOpFails(s.Cmd, err)
			s.RespWriter.WriteError(err)
		}
	}
	s.Stats.IncrOpStats(s.Cmd, startUnixNano)
	return err
}

func (s *Session) checkTxCommandNum() bool {
	if !s.TxCommandQueued {
		return true
	}
	switch s.Cmd {
	case WATCH, UNWATCH, MULTI, EXEC, DISCARD:
		return true
	case INFO, "shutdown", PING, PONG, ECHO, AUTH, SHUTDOWN:
		return true
	}
	return s.Recorder.CmdNum < TxCommandNumLimit
}

func (s *Session) SetTxCancel(err error) {
	if err == nil || !s.TxCommandQueued || s.TxState&TxStateCancel != 0 {
		return
	}
	if err == NotFoundErr || strings.Contains(err.Error(), "ERR wrong number of arguments") || err == TxGroupChangedErr {
		s.TxState |= TxStateCancel
	}
}

func (s *Session) SendTxQueued(err error) error {
	if err != nil {
		return err
	}
	s.RespWriter.WriteStatus(ReplyQUEUED)
	return nil
}

func (s *Session) SetTxClients(clients map[int]*InternalServerConn) {
	s.Recorder.init(clients)
}

func (s *Session) GetTxClients() map[int]*InternalServerConn {
	return s.Recorder.ServerClients
}

func (s *Session) ResetTx() {
	s.TxState = TxStateNone
	s.TxCommandQueued = false
	s.Recorder.Reset()
}
