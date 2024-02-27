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
	"io"
	"os"

	"github.com/zuoyebang/bitalostored/stored/engine"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/btools"
	"github.com/zuoyebang/bitalostored/stored/internal/config"

	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

func (s *Server) PrepareSnapshot() (ls interface{}, err error) {
	log.Info("start prepareSnapshot")
	if !s.syncDataDoing.CompareAndSwap(0, 1) {
		return ls, errors.New("prepare snapshot is running")
	}

	defer func() {
		s.syncDataDoing.Store(0)
		if err != nil {
			log.Errorf("server PrepareSnapshot fail err:%s", err.Error())
			s.Info.Stats.DbSyncErr = err.Error()
			s.Info.Stats.DbSyncStatus = DB_SYNC_PREPARE_FAIL
		}
	}()

	m := s.GetDB()
	if m.IsBitsdbClosed() {
		return ls, errors.New("bitsdb closed")
	}

	if !s.dbSyncing.CompareAndSwap(0, 1) {
		return ls, err
	}

	m.Flush(btools.FlushTypeCheckpoint, 0)

	m.CheckpointPrepareStart()
	defer func() {
		m.CheckpointPrepareEnd()
		if err != nil {
			s.dbSyncing.Store(0)
		}
	}()

	s.Info.Stats.DbSyncStatus = DB_SYNC_PREPARE_SUCC
	defer log.Cost("bitalos PrepareSnapshot DoSnapshot ")()
	snapshotPath := config.GetBitalosSnapshotPath()
	ls, err = m.DoSnapshot(snapshotPath)
	return ls, err
}

func (s *Server) SaveSnapshot(ctx interface{}, w io.Writer, done <-chan struct{}) error {
	db := s.GetDB()

	defer func() {
		s.dbSyncing.Store(0)
		db.CleanSnapshot()
	}()

	if !s.syncDataDoing.CompareAndSwap(0, 1) {
		return errors.New("save snapshot is running")
	}

	defer s.syncDataDoing.Store(0)
	s.Info.Stats.DbSyncRunning.Store(DB_SYNC_RUN_TYPE_SEND)
	s.Info.Stats.DbSyncErr = ""
	s.Info.Stats.DbSyncStatus = DB_SYNC_SENDING
	err := db.SaveSnapshot(ctx, w, done)
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

func (s *Server) RecoverFromSnapshot(r io.Reader, done <-chan struct{}) error {
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

	if err = os.Rename(dbsyncPath, dataPath); err != nil {
		s.Info.Stats.DbSyncErr = err.Error()
		log.Errorf("recoverFromSnapshot rename %s to %s fail err:%s", dbsyncPath, dataPath, s.Info.Stats.DbSyncErr)
		s.Info.Stats.DbSyncStatus = DB_SYNC_RECVING_FAIL
		return err
	}
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
