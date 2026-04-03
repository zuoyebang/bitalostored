// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"runtime"
	"time"
)

const (
	QueueOperation = "queueOperation"
)

type OperationRecorder interface {
	Create(url, uid, opData string, operationTime int64) error
}

type Queue struct {
	length            int
	qchans            chan *QData
	operationRecorder OperationRecorder
}

type QData struct {
	Url           string
	OpData        string
	Username      string
	OperationTime int64
}

func NewQueue(length int, recorder OperationRecorder) *Queue {
	queue := &Queue{
		length:            length,
		qchans:            make(chan *QData, length),
		operationRecorder: recorder,
	}
	queue.consume(queue.qchans)
	return queue
}

func (q *Queue) Close() {
	log.Infof("async queue is closing")
	close(q.qchans)
	log.Infof("async queue is closed")
}

func (q *Queue) QLength() int {
	return len(q.qchans)
}

func (q *Queue) Push(data *QData) error {
	q.qchans <- data
	return nil
}

func (q *Queue) consume(qchan chan *QData) {
	go func(qchan chan *QData) {
		defer func() {
			if e := recover(); e != nil {
				buf := make([]byte, 2048)
				n := runtime.Stack(buf, false)
				buf = buf[0:n]
				log.Errorf("queue run panic, [err:%v] [panic:%v]", e, string(buf))
				time.Sleep(100 * time.Millisecond)
				q.consume(qchan)
			}
		}()

		for {
			qdata, ok := <-qchan
			if !ok {
				log.Infof("receive close chan, close queue chan")
				break
			}
			if len(qdata.Username) <= 0 && len(qdata.OpData) <= 0 {
				continue
			}
			if q.operationRecorder != nil {
				err := q.operationRecorder.Create(qdata.Url, qdata.Username, qdata.OpData, qdata.OperationTime)
				if err != nil {
					log.Errorf("operation failed, data:%+v, err:%v", qdata, err)
				}
			}
		}
	}(qchan)
}
