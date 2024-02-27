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
	"github.com/zuoyebang/bitalostored/proxy/internal/errn"
	"github.com/zuoyebang/bitalostored/proxy/resp"
	"github.com/zuoyebang/bitalostored/proxy/router"
)

func init() {
	resp.Register(resp.WATCH, WatchCommand)
	resp.Register(resp.UNWATCH, UnwatchCommand)
	resp.Register(resp.MULTI, MultiCommand)
	resp.Register(resp.EXEC, ExecCommand)
	resp.Register(resp.DISCARD, DiscardCommand)
}

func createTxClients(proxyClient *router.ProxyClient, s *resp.Session) error {
	var err error
	var clients map[int]*resp.InternalServerConn
	clients, err = proxyClient.GetMasterClients()
	if err != nil {
		for _, c := range clients {
			c.Conn.Close()
		}
		return err
	}

	s.SetTxClients(clients)
	return nil
}

func WatchCommand(s *resp.Session) error {
	if !s.OpenDistributedTx {
		return errn.ErrTxDisable
	}
	args := s.Args
	if len(args) == 0 {
		return resp.CmdParamsErr(resp.WATCH)
	}
	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}
	if s.Recorder.ServerClients == nil {
		if err = createTxClients(proxyClient, s); err != nil {
			return err
		}
	}

	err = proxyClient.Watch(s, args)
	if err != nil {
		return err
	}
	s.TxState |= resp.TxStateWatch
	s.RespWriter.WriteStatus(resp.ReplyOK)
	return nil
}

func UnwatchCommand(s *resp.Session) error {
	if !s.OpenDistributedTx {
		return errn.ErrTxDisable
	}
	if len(s.Args) != 0 {
		return resp.CmdParamsErr(resp.UNWATCH)
	}
	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}
	if s.Recorder.ServerClients == nil {
		s.RespWriter.WriteStatus(resp.ReplyOK)
		return nil
	}

	proxyClient.Unwatch(s)
	s.ReleaseTxClients()
	s.TxState &= ^resp.TxStateWatch
	s.Recorder.Reset()
	s.RespWriter.WriteStatus(resp.ReplyOK)
	return nil
}

func MultiCommand(s *resp.Session) error {
	if !s.OpenDistributedTx {
		return errn.ErrTxDisable
	}
	if len(s.Args) != 0 {
		return resp.CmdParamsErr(resp.MULTI)
	}
	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}
	if s.Recorder.MultiOk {
		return errn.ErrMultiNested
	}

	if s.Recorder.ServerClients == nil {
		if err = createTxClients(proxyClient, s); err != nil {
			return err
		}
	}

	err = proxyClient.Multi(s)
	if err == nil {
		s.TxCommandQueued = true
		s.Recorder.MultiOk = true
		s.RespWriter.WriteStatus(resp.ReplyOK)
		return nil
	} else {
		return err
	}
}

func ExecCommand(s *resp.Session) error {
	if !s.OpenDistributedTx {
		return errn.ErrTxDisable
	}
	if len(s.Args) != 0 {
		return resp.CmdParamsErr(resp.EXEC)
	}
	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}
	defer func() {
		s.ReleaseTxClients()
		s.ResetTx()
	}()
	if s.TxState&resp.TxStateCancel != 0 {
		proxyClient.Discard(s)
		return resp.TxAbortErr
	}

	var prepareOk bool
	prepareOk, err = proxyClient.Exec(s)
	if !prepareOk {
		if err.Error() == errn.ErrWatchKeyChanged.Error() {
			s.RespWriter.WriteBulk(nil)
			return nil
		}
		return err
	}
	if s.Recorder.CmdNum == 0 {
		s.RespWriter.WriteStatus("(empty array)")
		return nil
	}

	cmdOutput := make([]interface{}, 0, len(s.Recorder.TxCommands))
	for _, r := range s.Recorder.TxCommands {
		cmdOutput = append(cmdOutput, r.Response)
	}
	if s.Recorder.CmdNum > resp.TxCommandNumLimit {
		cmdOutput = append(cmdOutput, errn.ErrTxCommandNumTooLarge)
	}
	s.RespWriter.WriteArray(cmdOutput)

	return nil
}

func DiscardCommand(s *resp.Session) error {
	if !s.OpenDistributedTx {
		return errn.ErrTxDisable
	}
	if len(s.Args) != 0 {
		return resp.CmdParamsErr(resp.DISCARD)
	}
	proxyClient, err := router.GetProxyClient()
	if err != nil {
		return err
	}
	if s.Recorder.ServerClients == nil {
		s.RespWriter.WriteStatus(resp.ReplyOK)
		return nil
	}

	proxyClient.Discard(s)
	s.RespWriter.WriteStatus(resp.ReplyOK)
	s.ReleaseTxClients()
	s.ResetTx()
	return nil
}
