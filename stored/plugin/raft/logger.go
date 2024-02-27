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
	"github.com/zuoyebang/bitalostored/raft/logger"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

var DefaultLogger = &DLog{}

type DLog struct {
}

func (l *DLog) SetLevel(logger.LogLevel) {}

func (l *DLog) Debugf(format string, args ...interface{}) {
	if log.IsDebug() {
		log.Outputf(log.TypeDebug, format, args...)
	}
}

func (l *DLog) Infof(format string, args ...interface{}) {
	log.Outputf(log.TypeInfo, format, args...)
}

func (l *DLog) Warningf(format string, args ...interface{}) {
	log.Outputf(log.TypeWarn, format, args...)
}

func (l *DLog) Errorf(format string, args ...interface{}) {
	log.Outputf(log.TypeError, format, args...)
}

func (l *DLog) Panicf(format string, args ...interface{}) {
	log.Outputf(log.TypeFatal, format, args...)
}
