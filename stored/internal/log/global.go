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

package log

import (
	"bytes"
	"fmt"
	"os"

	"github.com/zuoyebang/bitalostored/butils"
)

var log *Logger = &Logger{
	outWriter:  os.Stdout,
	errWriter:  os.Stderr,
	slowWriter: os.Stdout,
}

func IsDebug() bool {
	return log.debug
}

func CloseLog() {
	log.CloseSync()
}

func Outputf(level string, format string, arg ...interface{}) {
	if log == nil {
		return
	}

	log.outputf(level, format, arg...)
}

func SlowLog(remoteAddr string, cost int64, raftCost int64, request [][]byte, err error) {
	buffer := bytes.Buffer{}
	for i, arg := range request {
		buffer.Write(arg)
		if i != len(request)-1 {
			buffer.WriteByte(' ')
		}
		if buffer.Len() >= 128 {
			break
		}
	}

	var query []byte
	var status string

	if buffer.Len() > 256 {
		query = buffer.Bytes()[:256]
	} else {
		query = buffer.Bytes()
	}

	if err == nil {
		status = "OK"
	} else {
		status = "FAIL"
	}

	log.outputf(TypeSlow, slowFormat, remoteAddr, cost, raftCost, query, status)
}

func Info(arg ...interface{}) {
	log.output(TypeInfo, arg...)
}

func Warn(arg ...interface{}) {
	log.output(TypeWarn, arg...)
}

func Error(arg ...interface{}) {
	log.output(TypeError, arg...)
}

func Fatal(arg ...interface{}) {
	log.output(TypeFatal, arg...)
	os.Exit(1)
}

func Debug(arg ...interface{}) {
	if log.debug {
		log.output(TypeDebug, arg...)
	}
}

func Infof(format string, arg ...interface{}) {
	log.outputf(TypeInfo, format, arg...)
}

func Warnf(format string, arg ...interface{}) {
	log.outputf(TypeWarn, format, arg...)
}

func Errorf(format string, arg ...interface{}) {
	log.outputf(TypeError, format, arg...)
}

func Fatalf(format string, arg ...interface{}) {
	log.outputf(TypeFatal, format, arg...)
}

func Debugf(format string, arg ...interface{}) {
	if log.debug {
		log.outputf(TypeDebug, format, arg...)
	}
}

func Show(arg ...interface{}) {
	level := fmt.Sprint(arg[0])
	var list []interface{}
	for i := 1; i < len(arg)-1; i += 2 {
		if len(list) > 0 {
			list = append(list, " ")
		}
		list = append(list, arg[i], ": ", arg[i+1])
	}
	log.output(level, list...)
}

func Cost(arg ...interface{}) func(...func() []interface{}) {
	begin := now()
	return func(cb ...func() []interface{}) {
		ls := []interface{}{fmt.Sprint(arg...), " cost: ", butils.FmtDuration(now().Sub(begin))}
		for _, v := range cb {
			ls = append(ls, v()...)
		}
		log.output(TypeInfo, ls...)
	}
}
