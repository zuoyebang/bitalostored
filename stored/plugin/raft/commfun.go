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
	"reflect"
	"time"

	braft "github.com/zuoyebang/bitalostored/raft"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

type nodReqFun func(clusterID uint64, nodeID uint64, address string, configChangeIndex uint64,
	timeout time.Duration) (*braft.RequestState, error)

func (p *StartRun) doAddRaftNode(nodeId uint64, address string, n *int, max int, addReq nodReqFun) (RetType, error) {

	if *n >= max {
		err := " the number of retries has been reached"
		log.Error(err)
		return R_RETRY_EXHAUST, errors.New(err)
	}

	*n = *n + 1

	rs, err := addReq(p.Rc.ClusterID, nodeId, address, 0, p.TimeOut)
	if err != nil {
		log.Error("AddNode ", reflect.ValueOf(addReq).String(), " err: ", err)
		return R_ERROR, err
	}

	s := <-rs.AppliedC()
	if s.Timeout() || s.Dropped() {
		log.Warn(" retry, timeout: ", s.Timeout(), " dropped : ", s.Dropped())
		p.doAddRaftNode(nodeId, address, n, max, addReq)
	} else if s.Completed() {
		rs.Release()
	} else if s.Terminated() {
		err := "should be shutdown"
		log.Error(err)
		return R_SHUT_DOWN, errors.New(err)
	} else if s.Rejected() {
		err := " Rejected"
		log.Error(err)
		return R_REJECTED, errors.New(err)
	} else if s.Aborted() {
		err := "aborted"
		return R_ABORTED, errors.New(err)
	} else {
		return R_UNKNOWN_ERROR, errors.New(" unknown err")
	}
	return R_SUCCESS, nil
}

func (p *StartRun) addRaftNode(nodeId uint64, address string, retryisTime int, addReq nodReqFun) (RetType, error) {
	if !p.RaftReady {
		err := " raft is not ok "
		log.Error(err)
		return R_NIL_POINTER, errors.New(err)
	}

	if retryisTime < 1 {
		err := " retryisTime is too small "
		log.Error(err)
		return R_PARA_ERR, errors.New(err)
	}

	var n int = 0
	return p.doAddRaftNode(nodeId, address, &n, retryisTime, addReq)
}
