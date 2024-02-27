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
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
	"github.com/zuoyebang/bitalostored/stored/internal/resp"
	"github.com/zuoyebang/bitalostored/stored/server"
)

const (
	DefaultWorkNum     = 32
	DefaultQueueLength = 8 << 10
)

type Queue struct {
	worknum uint32
	length  uint32
	pD      *DiskKV
	qchans  []chan *QData
	wg      sync.WaitGroup
}

type QData struct {
	data      [][]byte
	isMigrate bool
	keyHash   uint32
}

func NewQueue(worknum, length int, pD *DiskKV) *Queue {
	if worknum < DefaultWorkNum {
		worknum = DefaultWorkNum
	}
	if length < DefaultQueueLength {
		length = DefaultQueueLength
	}

	queue := &Queue{
		worknum: uint32(worknum),
		length:  uint32(length),
		qchans:  make([]chan *QData, worknum),
		pD:      pD,
	}

	for i := 0; i < worknum; i++ {
		queue.qchans[i] = make(chan *QData, length)
		queue.consume(queue.qchans[i])
	}

	log.Infof("raft consume queue start worknum:%d length:%d", worknum, length)
	return queue
}

func (q *Queue) Close() {
	for i := range q.qchans {
		q.qchans[i] <- nil
	}
	q.wg.Wait()
	log.Infof("raft consume queue closed")
}

func (q *Queue) QLength() int {
	maxQueueLen := 0
	for i := range q.qchans {
		qLen := len(q.qchans[i])
		if maxQueueLen < qLen {
			maxQueueLen = qLen
		}
	}
	return maxQueueLen
}

func (q *Queue) push(data [][]byte, isMigrate bool, keyHash uint32) error {
	if len(data) < 2 || len(data[1]) <= 0 {
		return errors.New("raft consume queue push data err")
	}

	index := (keyHash + uint32(data[1][len(data[1])/2])) % q.worknum
	q.qchans[index] <- &QData{
		data:      data,
		isMigrate: isMigrate,
		keyHash:   keyHash,
	}

	return nil
}

func (q *Queue) consume(qchan chan *QData) {
	q.wg.Add(1)
	go func(qch chan *QData) {
		defer func() {
			q.wg.Done()
			if e := recover(); e != nil {
				buf := make([]byte, 2048)
				n := runtime.Stack(buf, false)
				buf = buf[0:n]
				log.Errorf("raft consume queue run panic err:%v panic:%s", e, string(buf))
				time.Sleep(100 * time.Millisecond)
				q.consume(qch)
			}
		}()

		for {
			qdata, ok := <-qch
			if !ok || qdata == nil {
				return
			}

			c := server.GetRaftClientFromPool(q.pD.s, qdata.data, qdata.keyHash)
			if c.Cmd == "script" {
				if len(c.Args) < 1 {
					log.Error("invalid script cmd")
					server.PutRaftClientToPool(c)
					continue
				}
				c.Cmd = c.Cmd + unsafe2.String(resp.LowerSlice(c.Args[0]))
			}
			if err := c.ApplyDB(0); err != nil {
				log.Errorf("qchans consume applydb fail command:%s err:%v", c.Cmd, err)
			}
			server.PutRaftClientToPool(c)
		}
	}(qchan)
}
