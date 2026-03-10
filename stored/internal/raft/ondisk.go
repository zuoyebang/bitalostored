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

package raft

import (
	"io"
	"unsafe"

	sm "github.com/zuoyebang/bitalosraft/statemachine"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/marshal/update"
	"github.com/zuoyebang/bitalostored/stored/server"
	"google.golang.org/protobuf/proto"
)

var UpdateOtherNodeDoing = []byte("&OtherNode*")
var UpdateSelfNodeDoing = []byte("&SelfNode*")

type DiskKV struct {
	clusterID uint64
	nodeID    uint64
	db        unsafe.Pointer
	s         *server.Server
	p         *RaftNode
	queue     *Queue
}

func (pD *DiskKV) Open(stopc <-chan struct{}) (uint64, uint64, error) {
	if pD.s.IsWitness {
		return 0, 0, nil
	}

	var index uint64
	m := pD.getMeta()
	if m != nil {
		index = m.GetUpdateIndex()
	}
	log.Infof("open read entry cluster:%d node:%d index:%d flushIndex:0", pD.clusterID, pD.nodeID, index)
	return index, 0, nil
}

func (pD *DiskKV) Update(es []sm.Entry) ([]sm.Entry, error) {
	var originUpdateIndex uint64
	var res []sm.Entry

	meta := pD.getMeta()
	if meta != nil {
		originUpdateIndex = meta.GetUpdateIndex()
	}

	for _, v := range es {
		if meta != nil && v.Index > originUpdateIndex {
			meta.SetUpdateIndex(v.Index)
		}
		if len(v.Cmd) == 0 {
			v.Result.Data = UpdateSelfNodeDoing
			res = append(res, v)
			continue
		}
		slice := &update.ByteSlice{}
		if err := proto.Unmarshal(v.Cmd, slice); err != nil {
			v.Result.Data = []byte(err.Error())
			res = append(res, v)
			continue
		}

		writeDB := func() bool {
			if pD.nodeID == *slice.NodeId {
				return false
			}
			return v.Index > originUpdateIndex
		}()

		if writeDB {
			pD.queue.push(slice.Data, *slice.KeyHash)
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
	return pD.s.PrepareSnapshot(pD.clusterID, pD.p.checkpoint)
}

func (pD *DiskKV) SaveSnapshot(ctx interface{}, w io.Writer, done <-chan struct{}) error {
	return pD.s.SaveSnapshot(ctx, w, done)
}

func (pD *DiskKV) RecoverFromSnapshot(r io.Reader, done <-chan struct{}) error {
	return pD.s.RecoverFromSnapshot(r, done, pD.p.ClusterId, pD.p.reloadMeta)
}

func (pD *DiskKV) OnDisk() bool {
	return true
}

func (pD *DiskKV) Close() error {
	return nil
}

func (pD *DiskKV) GetHash() (uint64, error) {
	return 0, nil
}

func NewDiskKV(clusterID uint64, nodeID uint64, s *server.Server, p *RaftNode) sm.IOnDiskStateMachine {
	d := &DiskKV{
		clusterID: clusterID,
		nodeID:    nodeID,
		s:         s,
		p:         p,
	}
	d.queue = p.queue
	return d
}

func (pD *DiskKV) getMeta() *NodeMeta {
	if pD.s == nil || pD.s.IsWitness {
		return nil
	}
	return pD.p.meta
}
