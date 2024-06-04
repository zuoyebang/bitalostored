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
	"context"
	"errors"

	"github.com/zuoyebang/bitalostored/raft/statemachine"
	"github.com/zuoyebang/bitalostored/stored/internal/errn"
)

func (p *StartRun) doPropose(msg []byte, n *int, max int) (RetType, error) {
	if *n >= max {
		return R_RETRY_EXHAUST, errors.New("the retry number has been reached")
	}

	*n = *n + 1

	if _, err := p.Nh.Propose(p.Nh.GetNoOPSession(p.Rc.ClusterID), msg, p.TimeOut); err != nil {
		return R_ERROR, err
	}

	return R_SUCCESS, nil
}

func (p *StartRun) SyncPropose(msg []byte) (statemachine.Result, error) {
	if !p.RaftReady {
		return statemachine.Result{}, errn.ErrRaftNotReady
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.TimeOut)
	res, err := p.Nh.SyncPropose(ctx, p.Nh.GetNoOPSession(p.Rc.ClusterID), msg)
	cancel()
	return res, err
}

func (p *StartRun) Propose(msg []byte, retryTime int) (RetType, error) {
	if !p.RaftReady {
		return R_NIL_POINTER, errn.ErrRaftNotReady
	}

	if retryTime < 1 {
		return R_PARA_ERR, errors.New("retry time is too small")
	}

	var n int = 0
	return p.doPropose(msg, &n, retryTime)
}
