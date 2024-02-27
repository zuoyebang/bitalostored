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

package raft

import (
	"io"
	"unsafe"

	sm "github.com/zuoyebang/bitalostored/raft/statemachine"
	"github.com/zuoyebang/bitalostored/stored/engine/bitsdb/dbmeta"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/marshal/update"
	"github.com/zuoyebang/bitalostored/stored/server"

	"github.com/golang/protobuf/proto"
)

var UpdateOtherNodeDoing = []byte("&OtherNode*")
var UpdateSelfNodeDoing = []byte("&SelfNode*")

type DiskKV struct {
	clusterID   uint64
	nodeID      uint64
	lastApplied uint64
	db          unsafe.Pointer
	closed      bool
	aborted     bool
	s           *server.Server
	p           *StartRun
	queue       *Queue
}

func (pD *DiskKV) Open(stopc <-chan struct{}) (uint64, uint64, error) {
	if pD.s.IsWitness {
		return 0, 0, nil
	}

	var index, flushIndex uint64
	meta := pD.getMeta()
	if meta == nil {
		log.Error("open raft get db meta is nil")
	} else {
		index = meta.GetUpdateIndex()
		flushIndex = meta.GetFlushIndex()
	}
	log.Infof("open read entry index:%d flushIndex:%d", index, flushIndex)
	return index, flushIndex, nil
}

func (pD *DiskKV) Update(es []sm.Entry) ([]sm.Entry, error) {
	if pD.s.GetIsClosed() {
		return nil, errn.ErrServerClosed
	}

	var originUpdateIndex uint64
	var originFlushIndex uint64
	var res []sm.Entry

	meta := pD.getMeta()
	if meta != nil {
		originUpdateIndex = meta.GetUpdateIndex()
		originFlushIndex = meta.GetFlushIndex()
	}

	for _, v := range es {
		if meta != nil && v.Index > originUpdateIndex {
			meta.SetUpdateIndex(v.Index)
		}
		slice := &update.ByteSlice{}
		if len(v.Cmd) == 0 {
			v.Result.Data = UpdateSelfNodeDoing
			res = append(res, v)
			continue
		}
		if err := proto.Unmarshal(v.Cmd, slice); err != nil {
			v.Result.Data = []byte(err.Error())
			res = append(res, v)
			continue
		}

		updateSelf := func() bool {
			if v.Index > originFlushIndex && v.Index <= originUpdateIndex {
				return true
			}
			if pD.nodeID == *slice.NodeId {
				return false
			}
			return true
		}()

		if updateSelf {
			pD.queue.push(slice.Data, *slice.IsMigrate, *slice.KeyHash)
			v.Result.Data = UpdateOtherNodeDoing
		} else {
			v.Result.Data = UpdateSelfNodeDoing
		}

		res = append(res, v)
	}
	return res, nil
}

func (pD *DiskKV) Lookup(key interface{}) (interface{}, error) {
	return nil, nil
}

func (pD *DiskKV) Sync() error {
	return nil
}

func (pD *DiskKV) PrepareSnapshot() (interface{}, error) {
	return pD.s.PrepareSnapshot()
}

func (pD *DiskKV) SaveSnapshot(ctx interface{}, w io.Writer, done <-chan struct{}) error {
	return pD.s.SaveSnapshot(ctx, w, done)
}

func (pD *DiskKV) RecoverFromSnapshot(r io.Reader, done <-chan struct{}) error {
	return pD.s.RecoverFromSnapshot(r, done)
}

func (pD *DiskKV) OnDisk() bool {
	return true
}

func (pD *DiskKV) Close() error {
	pD.queue.Close()
	pD.closed = true
	return nil
}

func (pD *DiskKV) GetHash() (uint64, error) {
	return 0, nil
}

func NewDiskKV(clusterID uint64, nodeID uint64, s *server.Server, p *StartRun) sm.IOnDiskStateMachine {
	d := &DiskKV{
		clusterID: clusterID,
		nodeID:    nodeID,
		s:         s,
		p:         p,
	}
	workers := config.GlobalConfig.RaftQueue.Workers
	length := config.GlobalConfig.RaftQueue.Length
	d.queue = NewQueue(workers, length, d)
	p.queue = d.queue
	return d
}

func (pD *DiskKV) getMeta() *dbmeta.Meta {
	if pD.s == nil || pD.s.IsWitness {
		return nil
	}
	db := pD.s.GetDB()
	if db == nil {
		return nil
	}
	return db.Meta
}
