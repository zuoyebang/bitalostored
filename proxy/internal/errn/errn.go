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

package errn

import "errors"

var (
	ErrMultiNested          = errors.New("err MULTI calls can not be nested")
	ErrTxDisable            = errors.New("err tx command disable")
	ErrTxCommandNumTooLarge = errors.New("err command num is out of range")
	ErrAntsPoolGetFail      = errors.New("err ants pools")
	ErrWatchKeyChanged      = errors.New("err watch key changed")
	ErrWatchResultErr       = errors.New("err watch result err")
	ErrMultiResultErr       = errors.New("err multi result err")
	ErrPrepareFail          = errors.New("err exec prepare fail")
	ErrClosedProxy          = errors.New("use of closed proxy")
	ErrNotInitProxy         = errors.New("not init proxy client")
	ErrInvalidSlotId        = errors.New("use of invalid slot id")
	ErrReturnNil            = errors.New("err return nil")
)
