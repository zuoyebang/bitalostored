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
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/zuoyebang/bitalostored/stored/engine"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/slowshield"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
	"golang.org/x/net/netutil"
)

const (
	StatusPrepare = iota
	StatusStart
	StatusRunning
	StatusClose
	StatusExited
)

type Server struct {
	closed         atomic.Bool
	status         int
	quit           chan struct{}
	isDebug        bool
	address        string
	listener       net.Listener
	dbSyncListener net.Listener
	connWait       sync.WaitGroup
	rcm            sync.RWMutex
	rcs            map[*Client]struct{}
	db             *engine.Bitalos
	slowQuery      *slowshield.SlowShield
	recoverLock    sync.Mutex
	syncDataDoing  atomic.Int32
	dbSyncing      atomic.Int32
	luaMu          []*sync.Mutex
	expireClosedCh chan struct{}
	expireWg       sync.WaitGroup

	Info              *SInfo
	IsMaster          func() bool
	MigrateDelToSlave func(keyHash uint32, data [][]byte) error
	IsWitness         bool

	openDistributedTx bool
	txLocks           *TxShardLocker
	txParallelCounter atomic.Int32
	txPrepareWg       sync.WaitGroup
}

func (s *Server) GetDB() *engine.Bitalos {
	if s.IsWitness {
		return nil
	}
	return s.db
}

func (s *Server) FlushCallback(compactIndex uint64) {
	db := s.GetDB()
	if db == nil {
		return
	}
	if !db.IsOpenRaftRestore() {
		return
	}
	db.Flush(btools.FlushTypeRemoveLog, compactIndex)
}

func (s *Server) addRespClient(c *Client) {
	s.rcm.Lock()
	s.Info.Client.ClientTotal.Add(1)
	s.Info.Client.ClientAlive.Add(1)
	s.rcs[c] = struct{}{}
	s.rcm.Unlock()
}

func (s *Server) delRespClient(c *Client) {
	s.rcm.Lock()
	s.Info.Client.ClientAlive.Add(-1)
	delete(s.rcs, c)
	s.rcm.Unlock()
}

func (s *Server) closeAllRespClients() {
	s.rcm.Lock()
	for c := range s.rcs {
		c.Close()
	}
	s.rcm.Unlock()
	s.txPrepareWg.Wait()
}

func (s *Server) Run() {
	l, err := net.Listen("tcp", s.address)
	if err != nil {
		log.Errorf("net listen fail err:%s", err.Error())
		return
	}

	s.Info.Server.ConfigFile = config.GlobalConfig.Server.ConfigFile
	s.Info.Server.StartTime = utils.GetCurrentTimeString()
	s.Info.Server.ServerAddress = s.address
	s.Info.Server.GitVersion = utils.Version
	s.Info.Server.Compile = utils.Compile
	s.Info.Server.MaxClient = config.GlobalConfig.Server.Maxclient
	s.Info.Server.MaxProcs = config.GlobalConfig.Server.Maxprocs
	s.Info.Server.ProcessId = os.Getpid()
	s.Info.Server.UpdateCache()

	productName := config.GlobalConfig.Server.ProductName
	var addr string
	if len(config.GlobalConfig.Server.Address) > 1 {
		addr = config.GlobalConfig.Server.Address[1:]
	}
	cpuCgroupPath := fmt.Sprintf("/sys/fs/cgroup/cpu/stored/server_%s_%s", productName, addr)
	cpuAdjuster := NewCpuAdjust(cpuCgroupPath, config.GlobalConfig.Server.Maxprocs)
	cpuAdjuster.Run(s)

	maxClientNum := int(config.GlobalConfig.Server.Maxclient)
	s.listener = netutil.LimitListener(l, maxClientNum)

	log.Infof("listen:%s maxClientNum:%d", s.address, maxClientNum)
	s.status = StatusStart
	runPluginStart(s)
	s.status = StatusRunning

	defer func() {
		s.status = StatusClose
	}()

	for {
		select {
		case <-s.quit:
			log.Info("bitalos server receive quit signal")
			return
		default:
			if c, e := s.listener.Accept(); e != nil {
				log.Errorf("accept err:%s", e.Error())
				continue
			} else {
				go NewClientRESP(c, s).run()
			}
		}
	}
}

func (s *Server) Close() {
	if s.closed.Load() {
		return
	}

	close(s.quit)
	close(s.expireClosedCh)

	s.listener.Close()
	s.closeAllRespClients()
	s.connWait.Wait()
	runPluginStop(s, recover())

	if !s.IsWitness {
		s.expireWg.Wait()
		s.GetDB().Close()
	}

	s.closed.Store(true)
	s.status = StatusExited
}

func (s *Server) GetIsClosed() bool {
	return s.closed.Load()
}

func NewServer() (*Server, error) {
	s := &Server{
		address:           config.GlobalConfig.Server.Address,
		isDebug:           config.GlobalConfig.Log.IsDebug,
		slowQuery:         slowshield.NewSlowShield(),
		quit:              make(chan struct{}),
		rcm:               sync.RWMutex{},
		rcs:               make(map[*Client]struct{}, 128),
		recoverLock:       sync.Mutex{},
		expireClosedCh:    make(chan struct{}),
		openDistributedTx: config.GlobalConfig.Server.OpenDistributedTx,
		IsWitness:         config.GlobalConfig.RaftCluster.IsWitness,
		Info:              NewSinfo(),
	}

	if s.IsWitness {
		return s, nil
	}

	if s.openDistributedTx {
		s.txLocks = NewTxLockers(200)
	}

	luaMux := make([]*sync.Mutex, LuaShardCount)
	for i := uint32(0); i < LuaShardCount; i++ {
		luaMux[i] = &sync.Mutex{}
	}
	s.luaMu = luaMux

	if err := os.MkdirAll(config.GetBitalosSnapshotPath(), 0755); err != nil {
		return nil, errors.Wrap(err, "mkdir snapshot err")
	}

	db, err := engine.NewBitalos(config.GetBitalosDbDataPath())
	if err != nil {
		return nil, errors.Wrap(err, "new bitalos err")
	}

	s.db = db
	s.RunDeleteExpireDataTask()

	return s, nil
}
