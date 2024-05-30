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
	"os"
)

var log = &Logger{
	outWriter:    os.Stdout,
	statsWriter:  os.Stdout,
	accessWriter: os.Stdout,
	slowWriter:   os.Stdout,
}

func IsDebug() bool {
	return log.debug
}

func CloseLog() {
	log.CloseSync()
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
		log.Debug(arg...)
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
	os.Exit(1)
}

func Debugf(format string, arg ...interface{}) {
	if log.debug {
		log.outputf(TypeDebug, format, arg...)
	}
}

func Access(remoteAddr string, usedTime int64, request []byte, err error) {
	log.Access(remoteAddr, usedTime, request, err)
}

func Slow(remoteAddr string, usedTimeUs int64, request []byte, err error) {
	log.Slow(remoteAddr, usedTimeUs, request, err)
}

func Stats(arg interface{}) {
	log.Stats(arg)
}
