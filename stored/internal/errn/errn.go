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
	"fmt"
)

var (
	ErrSyntax                 = errors.New("ERR syntax error")
	ErrLenArg                 = errors.New("ERR args len is wrong")
	ErrTxDisable              = errors.New("ERR tx command disable")
	ErrWatchKeyChanged        = errors.New("ERR watch key changed")
	ErrPrepareLockFail        = errors.New("ERR prepare lock fail")
	ErrPrepareLockTimeout     = errors.New("ERR prepare lock timeout")
	ErrTxNotInMaster          = errors.New("ERR tx in slave node")
	ErrMultiNested            = errors.New("ERR MULTI calls can not be nested")
	ErrTxQpsLimit             = errors.New("ERR tx qps too high")
	ErrPrepareNoMulti         = errors.New("ERR PREPARE without MULTI")
	ErrPrepareNested          = errors.New("ERR PREPARE calls can not be nested")
	ErrExecNotPrepared        = errors.New("ERR Exec not prepared")
	ErrDiscardNoMulti         = errors.New("ERR DISCARD without MULTI")
	ErrProtocol               = errors.New("invalid request")
	ErrRaftNotReady           = errors.New("raft is not ready")
	ErrWrongType              = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrKeySize                = errors.New("invalid key size")
	ErrValueSize              = errors.New("invalid value size")
	ErrArgsEmpty              = errors.New("invalid args empty")
	ErrFieldSize              = errors.New("invalid field size")
	ErrExpireValue            = errors.New("invalid expire value")
	ErrZSetScoreRange         = errors.New("invalid zset score range")
	ErrZsetMemberNil          = errors.New("zset member is nil")
	ErrClientQuit             = errors.New("remote client quit")
	ErrSlotIdNotMatch         = errors.New("migrate slotId not match")
	ErrMigrateRunning         = errors.New("migrate running")
	ErrDataType               = errors.New("not support dataType")
	ErrDbSyncFailRefuse       = errors.New("ERR db syncing/fail, refuse request")
	ErrNotImplement           = errors.New("command not implement")
	ErrRangeOffset            = errors.New("ERR offset is out of range")
	ErrValue                  = errors.New("ERR value is not an integer or out of range")
	ErrInvalidRangeItem       = errors.New("ERR min or max not valid string range item")
	ErrBitOffset              = errors.New("ERR bit offset is not an integer or out of range")
	ErrBitValue               = errors.New("ERR bit is not an integer or out of range")
	ErrBitUnmarshal           = errors.New("ERR bitmap unmarshal fail")
	ErrBitMarshal             = errors.New("ERR bitmap marshal fail")
	ErrSlowShield             = errors.New("slow query shield, wait 1s to retry")
	ErrUnbalancedQuotes       = errors.New("ERR unbalanced quotes in request")
	ErrInvalidBulkLength      = errors.New("ERR invalid bulk length")
	ErrInvalidMultiBulkLength = errors.New("ERR invalid multibulk length")
)

func CmdEmptyErr(cmd string) error {
	return fmt.Errorf("ERR empty command for '%s' command", cmd)
}

func CmdParamsErr(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}
