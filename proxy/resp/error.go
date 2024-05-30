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

package resp

import (
	"errors"
	"fmt"
)

var (
	EmptyCommandErr           = errors.New("empty command")
	NotFoundErr               = errors.New("command not found")
	NotImplementErr           = errors.New("command not implement")
	NotAuthenticatedErr       = errors.New("not authenticated")
	AuthenticationFailureErr  = errors.New("authentication failure")
	WriteErrorOnReadOnlyProxy = errors.New("Proxy Support READ COMMAND ONLY")
	RangeOffsetErr            = errors.New("ERR offset is out of range")
	BitOffsetErr              = errors.New("ERR bit offset is not an integer or out of range")
	SyntaxErr                 = errors.New("ERR syntax error")
	ValueErr                  = errors.New("ERR value is not an integer or out of range")
	FloatErr                  = errors.New("ERR value is not a valid float")
	HashTagErr                = errors.New("ERR hashtag mismatch or missing")
	TxGroupChangedErr         = errors.New("ERR group changed in tx")
	TxAbortErr                = errors.New("EXECABORT Transaction discarded because of previous errors.")
)

func CmdParamsErr(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}
