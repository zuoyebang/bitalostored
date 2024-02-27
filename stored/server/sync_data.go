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
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"

	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

var buildConn atomic.Int32

var dbSyncRunningErr = errors.New("db sync is running")

func (s *Server) buildDbSyncListener() (string, error) {
	if buildConn.CompareAndSwap(0, 1) {
		defer buildConn.Store(0)
		if s.dbSyncing.Load() == 1 {
			return "", errors.New("db syncing conflicts")
		}

		if s.dbSyncListener == nil {
			address, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:0", "0.0.0.0"))
			if err != nil {
				return "", err
			}
			s.dbSyncListener, err = net.ListenTCP("tcp", address)
			if err != nil {
				return "", err
			}
			go func() {
				defer func() {
					s.dbSyncListener.Close()
					s.dbSyncListener = nil
				}()

				if conn, err := s.dbSyncListener.Accept(); err != nil {
					s.Info.Stats.DbSyncErr = err.Error()
					s.Info.Stats.DbSyncStatus = DB_SYNC_CONN_FAIL
					log.Error("accept dbsync conn err: ", err)
				} else {
					s.Info.Stats.DbSyncRunning.Store(1)
					if err := s.sendEngineData(conn); err != nil {
						log.Error("send engine conn err: ", err)
					}
					conn.Close()
					s.Info.Stats.DbSyncRunning.Store(0)
				}
			}()
		}
		return s.dbSyncListener.Addr().String(), nil
	}
	return "", errors.New("current is build db sync")
}

func (s *Server) buildDbAsyncConn(address string) error {
	if buildConn.CompareAndSwap(0, 1) {
		go func() {
			defer buildConn.Store(0)
			s.Info.Stats.DbSyncStatus = DB_SYNC_CONN_SUCC
			tcpAddr, err := net.ResolveTCPAddr("tcp", address)
			if err != nil {
				s.Info.Stats.DbSyncErr = err.Error()
				s.Info.Stats.DbSyncStatus = DB_SYNC_CONN_FAIL
				log.Errorf("resolve tcp address : %s err ：%s", tcpAddr, err.Error())
				return
			}
			conn, err := net.DialTCP("tcp", nil, tcpAddr)
			if conn != nil {
				defer conn.Close()
			}

			if err != nil {
				s.Info.Stats.DbSyncErr = err.Error()
				s.Info.Stats.DbSyncStatus = DB_SYNC_CONN_FAIL
				log.Errorf("build err conn err ：%s", err.Error())
				return
			}
			if err := s.RecoverFromSnapshot(conn, nil); err != nil {
				s.Info.Stats.DbSyncErr = err.Error()
				s.Info.Stats.DbSyncStatus = DB_SYNC_CONN_FAIL
				log.Errorf("build conn recover from snapshot err : %v", err)
				return
			}
		}()
		return nil
	} else {
		return dbSyncRunningErr
	}
}

func (s *Server) sendEngineData(w io.Writer) error {
	ls, err := s.PrepareSnapshot()
	if err != nil {
		log.Error("dbsync prepare snapshot", err)
		return err
	}

	if err = s.SaveSnapshot(ls, w, nil); err != nil {
		return err
	}
	return nil
}
