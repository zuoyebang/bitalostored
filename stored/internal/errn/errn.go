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

package errn

import (
	"errors"
)

var (
	ErrSyntax             = errors.New("Err syntax error")
	ErrLenArg             = errors.New("Err args len is wrong")
	ErrTxDisable          = errors.New("Err tx command disable")
	ErrWatchKeyChanged    = errors.New("Err watch key changed")
	ErrPrepareLockFail    = errors.New("Err prepare lock fail")
	ErrPrepareLockTimeout = errors.New("Err prepare lock timeout")
	ErrTxNotInMaster      = errors.New("ERR tx in slave node")
	ErrMultiNested        = errors.New("ERR MULTI calls can not be nested")
	ErrTxQpsLimit         = errors.New("ERR tx qps too high")
	ErrPrepareNoMulti     = errors.New("ERR PREPARE without MULTI")
	ErrPrepareNested      = errors.New("ERR PREPARE calls can not be nested")
	ErrExecNotPrepared    = errors.New("ERR Exec not prepared")
	ErrDiscardNoMulti     = errors.New("ERR DISCARD without MULTI")
	ErrProtocol           = errors.New("invalid request")
	ErrServerClosed       = errors.New("server is closed")
	ErrRaftNotReady       = errors.New("raft is not ready")
	ErrWrongType          = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrKeySize            = errors.New("invalid key size")
	ErrValueSize          = errors.New("invalid value size")
	ErrArgsEmpty          = errors.New("invalid args empty")
	ErrFieldSize          = errors.New("invalid field size")
	ErrExpireValue        = errors.New("invalid expire value")
	ErrZSetScoreRange     = errors.New("invalid zset score range")
	ErrZsetMemberNil      = errors.New("zset member is nil")
	ErrClientQuit         = errors.New("remote client quit")
	ErrSlotIdNotMatch     = errors.New("migrate slotId not match")
	ErrMigrateRunning     = errors.New("migrate running")
)
