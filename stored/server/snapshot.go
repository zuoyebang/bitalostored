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
	"io"
	"os"
	"path"

	"github.com/zuoyebang/bitalostored/stored/engine"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/trycatch"
)

func (s *Server) PrepareSnapshot(clusterId uint64, nodePrepare func(string) error) (ls interface{}, err error) {
	if !s.syncDataDoing.CompareAndSwap(0, 1) {
		return ls, errors.New("prepare snapshot is running")
	}

	defer func() {
		s.syncDataDoing.Store(0)
		if err != nil {
			log.Errorf("server PrepareSnapshot fail err:%s", err)
			s.Info.Stats.DbSyncErr = err.Error()
			s.Info.Stats.DbSyncStatus = DB_SYNC_PREPARE_FAIL
		}
	}()

	m := s.GetDB()
	if m.IsBitsdbClosed() {
		return ls, errn.ErrBitsdbClosed
	}

	if !s.dbSyncing.CompareAndSwap(0, 1) {
		return ls, err
	}

	defer func() {
		if err != nil {
			s.dbSyncing.Store(0)
			if sd, ok := ls.(*engine.SnapshotDetail); ok {
				sd.Clean()
			}
		}
	}()

	var ckCloser func()
	snapshotRoot := config.GetBitalosSnapshotPath()
	ls, ckCloser, err = m.DoSnapshot(snapshotRoot, nodePrepare, clusterId)
	if err != nil {
		return nil, err
	}

	s.snapshotDoneCh = make(chan struct{})
	wait := make(chan struct{})
	go func() {
		close(wait)
		select {
		case <-s.snapshotDoneCh:
			log.Info("snapshot finished")
			func() {
				trycatch.Panic("PrepareSnapshot ckCloser", recover())
				ckCloser()
			}()
		}
		s.dbSyncing.Store(0)
		m.CleanSnapshot(clusterId)
	}()

	<-wait
	s.Info.Stats.DbSyncStatus = DB_SYNC_PREPARE_SUCC

	return ls, nil
}

func (s *Server) SaveSnapshot(ctx interface{}, w io.Writer, done <-chan struct{}) error {
	if !s.syncDataDoing.CompareAndSwap(0, 1) {
		return errors.New("save snapshot is running")
	}

	defer func() {
		close(s.snapshotDoneCh)
		s.syncDataDoing.Store(0)
	}()

	s.Info.Stats.DbSyncRunning.Store(DB_SYNC_RUN_TYPE_SEND)
	s.Info.Stats.DbSyncErr = ""
	s.Info.Stats.DbSyncStatus = DB_SYNC_SENDING
	err := s.GetDB().SaveSnapshot(ctx, w, done)
	if err != nil {
		s.Info.Stats.DbSyncErr = err.Error()
		s.Info.Stats.DbSyncStatus = DB_SYNC_SEND_FAIL
	} else {
		s.Info.Stats.DbSyncErr = ""
		s.Info.Stats.DbSyncStatus = DB_SYNC_SEND_SUCC
	}
	s.Info.Stats.DbSyncRunning.Store(DB_SYNC_RUN_TYPE_END)
	return err
}

func (s *Server) RecoverFromSnapshot(r io.Reader, done <-chan struct{}, clusterId uint64, reloadMeta func() error) error {
	s.Info.Stats.DbSyncErr = ""
	s.Info.Stats.DbSyncStatus = DB_SYNC_RECVING
	s.Info.Stats.DbSyncRunning.Store(DB_SYNC_RUN_TYPE_RECV)

	s.recoverLock.Lock()
	defer func() {
		s.recoverLock.Unlock()
		s.Info.Stats.DbSyncRunning.Store(DB_SYNC_RUN_TYPE_END)
	}()

	s.GetDB().Close()
	log.Info("recoverFromSnapshot db syncing closed old db success")

	dataPath := config.GetBitalosDbDataPath()
	if err := os.RemoveAll(dataPath); err != nil {
		log.Errorf("recoverFromSnapshot remove old data dir %s err:%v", dataPath, err)
		s.Info.Stats.DbSyncErr = err.Error()
		s.Info.Stats.DbSyncStatus = DB_SYNC_RECVING_FAIL
	} else {
		log.Infof("recoverFromSnapshot remove old data dir succ %s", dataPath)
	}

	oldSsPath := config.GetBitalosSnapshotPath()
	if err := os.RemoveAll(oldSsPath); err != nil {
		log.Errorf("recoverFromSnapshot remove old snapshot dir fail path:%s err:%s", oldSsPath, err.Error())
	} else {
		log.Infof("recoverFromSnapshot remove old snapshot dir succ path:%s", oldSsPath)
	}

	dbsyncPath, err := s.GetDB().RecoverFromSnapshot(r, done)
	if err != nil {
		s.Info.Stats.DbSyncErr = err.Error()
		s.Info.Stats.DbSyncStatus = DB_SYNC_RECVING_FAIL
		return err
	}

	raftMetaFile := config.GetRaftNodeMetaFile(clusterId)
	syncMetaFile := path.Join(dbsyncPath, raftMetaFile)
	dstMetaFile := path.Join(config.GetRaftMetaPath(), raftMetaFile)
	if err = os.Rename(syncMetaFile, dstMetaFile); err != nil {
		s.Info.Stats.DbSyncErr = err.Error()
		log.Errorf("recoverFromSnapshot rename %s to %s fail err:%s", syncMetaFile, dstMetaFile, s.Info.Stats.DbSyncErr)
		s.Info.Stats.DbSyncStatus = DB_SYNC_RECVING_FAIL
		return err
	}
	if err = os.Rename(dbsyncPath, dataPath); err != nil {
		s.Info.Stats.DbSyncErr = err.Error()
		log.Errorf("recoverFromSnapshot rename %s to %s fail err:%s", dbsyncPath, dataPath, s.Info.Stats.DbSyncErr)
		s.Info.Stats.DbSyncStatus = DB_SYNC_RECVING_FAIL
		return err
	}
	reloadMeta()
	log.Infof("recoverFromSnapshot rename %s to %s success", dbsyncPath, dataPath)

	db, err := engine.NewBitalos(dataPath)
	if err != nil {
		s.Info.Stats.DbSyncErr = err.Error()
		log.Errorf("recoverFromSnapshot new db fail err:%s", s.Info.Stats.DbSyncErr)
		s.Info.Stats.DbSyncStatus = DB_SYNC_RECVING_FAIL
		return err
	}
	log.Info("recoverFromSnapshot new db succ")

	s.db = db

	s.Info.Stats.DbSyncErr = ""
	s.Info.Stats.DbSyncStatus = DB_SYNC_RECVING_SUCC
	return nil
}
