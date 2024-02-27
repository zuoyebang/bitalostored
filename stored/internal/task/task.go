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

package task

import (
	"github.com/zuoyebang/bitalostored/stored/internal/log"

	"sync/atomic"
	"time"
)

const (
	StatusWait = iota
	StatusDone
	StatusError
)

type Task struct {
	Arg interface{}
	CB  func(*Task) error
	ID  int64

	Error  error
	Status int

	BeginTime  time.Time
	StartTime  time.Time
	FinishTime time.Time
}

func (task *Task) Run() {
	log.Show("task", "run", log.FileLine(task.CB, 3), "id", task.ID, "arg", task.Arg)
	defer log.Cost("task ", log.FileLine(task.CB, 3), " id: ", task.ID, " arg", task.Arg, " err: ", task.Error, " ")()

	task.StartTime = time.Now()
	defer func() { task.FinishTime = time.Now() }()

	if e := task.CB(task); e != nil {
		task.Status = StatusError
		task.Error = e
		return
	}

	task.Status = StatusDone
}

var TaskID int64

func Run(arg interface{}, cb func(*Task) error) *Task {
	TaskID = atomic.AddInt64(&TaskID, 1)

	task := &Task{ID: TaskID, Arg: arg, CB: cb, BeginTime: time.Now()}
	go func() {
		defer func() {
			if e := recover(); e != nil {
				log.Warn(e)
			}
		}()
		task.Run()
	}()
	return task
}
