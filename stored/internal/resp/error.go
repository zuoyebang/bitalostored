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

package resp

import (
	"errors"
	"fmt"
)

var (
	ErrNotImplement     = errors.New("command not implement")
	ErrSyntax           = errors.New("ERR syntax error")
	ErrRangeOffset      = errors.New("ERR offset is out of range")
	ErrValue            = errors.New("ERR value is not an integer or out of range")
	ErrInvalidRangeItem = errors.New("ERR min or max not valid string range item")
	ErrBitOffset        = errors.New("ERR bit offset is not an integer or out of range")
	ErrBitValue         = errors.New("ERR bit is not an integer or out of range")
	ErrBitUnmarshal     = errors.New("ERR bitmap unmarshal fail")
	ErrBitMarshal       = errors.New("ERR bitmap marshal fail")
	ErrSlowShield       = errors.New("slow query shield, wait 1s to retry")
)

func CmdEmptyErr(cmd string) error {
	return fmt.Errorf("ERR empty command for '%s' command", cmd)
}

func CmdParamsErr(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}
