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
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/panjf2000/gnet/v2"
	"github.com/zuoyebang/bitalostored/stored/engine"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/bitsdb"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/internal/slowshield"
	"github.com/zuoyebang/bitalostored/stored/internal/trycatch"
	"github.com/zuoyebang/bitalostored/stored/internal/utils"
)

const errorReadEOF = "read: EOF"

type Server struct {
	*gnet.BuiltinEventEngine
	eng               gnet.Engine
	Info              *SInfo
	IsMaster          func() bool
	MigrateDelToSlave func(keyHash uint32, data [][]byte) error
	IsWitness         bool
	DoRaftSync        func(keyHash uint32, data [][]byte) ([]byte, error)
	DoRaftStop        func()
	laddr             string
	db                *engine.Bitalos
	closed            atomic.Bool
	quit              chan struct{}
	isDebug           bool
	isOpenRaft        bool
	slowQuery         *slowshield.SlowShield
	recoverLock       sync.Mutex
	syncDataDoing     atomic.Int32
	dbSyncing         atomic.Int32
	luaMu             []*sync.Mutex
	expireClosedCh    chan struct{}
	expireWg          sync.WaitGroup
	openDistributedTx bool
	txLocks           *TxShardLocker
	txParallelCounter atomic.Int32
	txPrepareWg       sync.WaitGroup
	cpu               *cpuAdjust
}

func NewServer() (*Server, error) {
	s := &Server{
		laddr:             config.GlobalConfig.Server.Address,
		isDebug:           config.GlobalConfig.Log.IsDebug,
		slowQuery:         slowshield.NewSlowShield(),
		quit:              make(chan struct{}),
		recoverLock:       sync.Mutex{},
		expireClosedCh:    make(chan struct{}),
		openDistributedTx: config.GlobalConfig.Server.OpenDistributedTx,
		isOpenRaft:        config.GlobalConfig.Plugin.OpenRaft,
		IsWitness:         config.GlobalConfig.RaftCluster.IsWitness,
	}
	s.Info = &SInfo{
		Client:         SinfoClient{cache: make([]byte, 0, 256)},
		Cluster:        SinfoCluster{cache: make([]byte, 0, 2048)},
		Stats:          SinfoStats{cache: make([]byte, 0, 2048)},
		Data:           SinfoData{cache: make([]byte, 0, 1024)},
		RuntimeStats:   SRuntimeStats{cache: make([]byte, 0, 3072)},
		BitalosdbUsage: bitsdb.NewBitsUsage(),
		Server: SinfoServer{
			cache:         make([]byte, 0, 2048),
			AutoCompact:   true,
			ConfigFile:    config.GlobalConfig.Server.ConfigFile,
			StartTime:     utils.GetCurrentTimeString(),
			ServerAddress: s.laddr,
			GitVersion:    utils.Version,
			Compile:       utils.Compile,
			MaxClient:     config.GlobalConfig.Server.Maxclient,
			ProcessId:     os.Getpid(),
		},
	}
	s.Info.Server.UpdateCache()

	RunCpuAdjuster(s)

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

func (s *Server) Close() {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}

	close(s.quit)
	close(s.expireClosedCh)

	if s.eng.Validate() == nil {
		if err := s.eng.Stop(context.TODO()); err != nil {
			log.Errorf("server gnet stop error %s", err)
		}
	}

	s.txPrepareWg.Wait()
	s.DoRaftStop()

	if !s.IsWitness {
		s.expireWg.Wait()
		s.GetDB().Close()
	}
}

func (s *Server) IsClosed() bool {
	return s.closed.Load()
}

func (s *Server) ListenAndServe() {
	gnetOptions := gnet.Options{
		Logger:          log.GetLogger(),
		Multicore:       true,
		ReusePort:       true,
		ReuseAddr:       true,
		EdgeTriggeredIO: config.GlobalConfig.Server.DisableEdgeTriggered,
	}

	if config.GlobalConfig.Server.NetEventLoopNum > 0 {
		gnetOptions.NumEventLoop = config.GlobalConfig.Server.NetEventLoopNum
	}

	if config.GlobalConfig.Server.NetWriteBuffer > 0 {
		gnetOptions.WriteBufferCap = config.GlobalConfig.Server.NetWriteBuffer.AsInt()
	}

	log.Infof("server gnet options NumEventLoop:%d EdgeTriggeredIO:%v WriteBufferCap:%d",
		gnetOptions.NumEventLoop, gnetOptions.EdgeTriggeredIO, gnetOptions.WriteBufferCap)

	if err := gnet.Run(s, fmt.Sprintf("tcp://%s", s.laddr), gnet.WithOptions(gnetOptions)); err != nil {
		log.Errorf("server gnet run error %s", err)
	}
}

func (s *Server) OnBoot(eng gnet.Engine) (action gnet.Action) {
	s.eng = eng
	return gnet.None
}

func (s *Server) OnOpen(conn gnet.Conn) (out []byte, action gnet.Action) {
	client := newConnClient(s, conn.RemoteAddr().String())
	conn.SetContext(client)
	return
}

func (s *Server) OnClose(conn gnet.Conn, err error) (action gnet.Action) {
	if client, ok := conn.Context().(*Client); ok {
		client.Close()
	}

	if err != nil && err.Error() != errorReadEOF {
		log.Errorf("conn OnClose error %s", err)
	}

	return gnet.None
}

func (s *Server) OnTraffic(conn gnet.Conn) (action gnet.Action) {
	defer func() {
		trycatch.Panic("conn OnTraffic", recover())
	}()

	client, ok := conn.Context().(*Client)
	if !ok {
		log.Error("conn OnTraffic get Client fail")
		return gnet.Close
	}

	dbSyncStatus := client.server.Info.Stats.DbSyncStatus
	if dbSyncStatus == DB_SYNC_RECVING_FAIL || dbSyncStatus == DB_SYNC_RECVING {
		client.Writer.WriteError(errn.ErrDbSyncFailRefuse)
		client.Writer.FlushToWriterIO(conn)
		log.Errorf("conn OnTraffic error %s", errn.ErrDbSyncFailRefuse)
		return gnet.Close
	}

	readBuf, _ := conn.Next(-1)
	if client.Reader.Len() > 0 {
		client.Reader.Write(readBuf)
		readBuf = client.Reader.Bytes()
	}

	cmds, writeBackBytes, err := resp.ParseCommands(readBuf[client.Reader.Offset:], client.ParseMarks[:0])
	if err != nil {
		client.Writer.WriteError(err)
		client.Writer.FlushToWriterIO(conn)
		log.Errorf("conn OnTraffic parse commands error %s", err)
		return gnet.Close
	}

	for i := range cmds {
		if err = client.HandleRequest(cmds[i].Args, false); err != nil {
			log.Errorf("conn OnTraffic handle request error %s", err)
		}

		if _, err = client.Writer.FlushToWriterIO(conn); err != nil {
			log.Errorf("conn OnTraffic write error %s", err)
		}
	}

	writeBackBytesLen := len(writeBackBytes)
	if writeBackBytesLen > 0 && client.Reader.Len() == 0 {
		client.Reader.Write(writeBackBytes)
	}

	if cmds != nil {
		client.Reader.Offset = client.Reader.Len() - writeBackBytesLen
	}

	if writeBackBytesLen == 0 {
		client.Reader.Reset()
		client.Reader.Offset = 0
	}

	return gnet.None
}
