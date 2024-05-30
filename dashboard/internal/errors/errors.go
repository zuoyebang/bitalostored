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

package errors

import (
	"errors"
	"fmt"

	"github.com/zuoyebang/bitalostored/dashboard/internal/trace"
)

var TraceEnabled = true

type TracedError struct {
	Stack trace.Stack
	Cause error
}

func (e *TracedError) Error() string {
	return e.Cause.Error()
}

func New(s string) error {
	return errors.New(s)
}

func Trace(err error) error {
	if err == nil || !TraceEnabled {
		return err
	}
	_, ok := err.(*TracedError)
	if ok {
		return err
	}
	return &TracedError{
		Stack: trace.TraceN(1, 32),
		Cause: err,
	}
}

func Errorf(format string, v ...interface{}) error {
	err := fmt.Errorf(format, v...)
	if !TraceEnabled {
		return err
	}
	return &TracedError{
		Stack: trace.TraceN(1, 32),
		Cause: err,
	}
}

func Stack(err error) trace.Stack {
	if err == nil {
		return nil
	}
	e, ok := err.(*TracedError)
	if ok {
		return e.Stack
	}
	return nil
}
