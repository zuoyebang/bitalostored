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
	"fmt"
	"os"
	"path"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/zuoyebang/bitalostored/butils"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	TypeInfo  = "INFO"
	TypeWarn  = "WARN"
	TypeError = "ERROR"
	TypeFatal = "FATAL"
	TypeDebug = "DEBUG"
	TypeSlow  = "SLOW"
)

const (
	headerFormat = "%s.%06d %s [%s] "
	slowFormat   = "[ip_port:%s] [duration(us):%d] [raftsync(us):%d] [query:%q] [status:%s]"
)

type Logger struct {
	debug      bool
	outWriter  zapcore.WriteSyncer
	errWriter  zapcore.WriteSyncer
	slowWriter zapcore.WriteSyncer
	outLogger  *zap.Logger
	errLogger  *zap.Logger
	slowLogger *zap.Logger
}

func (l *Logger) CloseSync() {
	if l.outLogger != nil {
		l.outLogger.Sync()
	}
	if l.errLogger != nil {
		l.errLogger.Sync()
	}
	if l.slowLogger != nil {
		l.slowLogger.Sync()
	}
}

func (l *Logger) filter(level string) bool {
	if l.debug {
		return false
	}
	if level == TypeDebug {
		return true
	}
	return false
}

func (l *Logger) formatHeader(level string) string {
	nowTime := time.Now()
	return fmt.Sprintf(headerFormat,
		nowTime.Format(time.DateTime), int64(nowTime.Nanosecond()/1000),
		FileLine(5, 2),
		level)
}

func (l *Logger) getWriteLogger(level string) *zap.Logger {
	var file *zap.Logger
	switch level {
	case TypeError, TypeFatal:
		file = l.errLogger
	case TypeSlow:
		file = l.slowLogger
	default:
		file = l.outLogger
	}
	return file
}

func (l *Logger) output(level string, arg ...interface{}) {
	if l.filter(level) {
		return
	}

	output := l.getWriteLogger(level)
	if output != nil {
		output.Info(fmt.Sprint(l.formatHeader(level), fmt.Sprint(arg...)))
	}
}

func (l *Logger) outputf(level string, ft string, arg ...interface{}) {
	if l.filter(level) {
		return
	}

	output := l.getWriteLogger(level)
	if output != nil {
		output.Info(fmt.Sprint(l.formatHeader(level), fmt.Sprintf(ft, arg...)))
	}
}

func (l *Logger) Info(arg ...interface{}) {
	l.output(TypeInfo, arg...)
}

func (l *Logger) Warn(arg ...interface{}) {
	l.output(TypeWarn, arg...)
}

func (l *Logger) Error(arg ...interface{}) {
	l.output(TypeError, arg...)
}

func (l *Logger) Fatal(arg ...interface{}) {
	l.output(TypeFatal, arg...)
}

func (l *Logger) Debug(arg ...interface{}) {
	if l.debug {
		l.output(TypeDebug, arg...)
	}
}

func (l *Logger) Infof(ft string, arg ...interface{}) {
	l.outputf(TypeInfo, ft, arg...)
}

func (l *Logger) Warnf(ft string, arg ...interface{}) {
	l.outputf(TypeWarn, ft, arg...)
}

func (l *Logger) Errorf(ft string, arg ...interface{}) {
	l.outputf(TypeError, ft, arg...)
}

func (l *Logger) Fatalf(ft string, arg ...interface{}) {
	l.outputf(TypeFatal, ft, arg...)
}

func (l *Logger) Debugf(ft string, arg ...interface{}) {
	if l.debug {
		l.outputf(TypeDebug, ft, arg...)
	}
}

func (l *Logger) Cost(arg ...interface{}) func() {
	begin := now()
	return func() {
		l.output(TypeInfo, fmt.Sprint(arg...), " cost: ", butils.FmtDuration(now().Sub(begin)))
	}
}

func (l *Logger) Show(arg ...interface{}) {
	level := fmt.Sprint(arg[0])
	var list []interface{}
	for i := 1; i < len(arg); i += 2 {
		if len(list) > 0 {
			list = append(list, " ")
		}
		list = append(list, arg[i], ": ", arg[i+1])
	}
	l.output(level, list...)
}

type LevelEnable struct {
}

func (le *LevelEnable) Enabled(l zapcore.Level) bool {
	return true
}

type Options struct {
	IsDebug      bool
	LogPath      string
	RotationTime string
}

func NewLogger(opts *Options) *Logger {
	logPath := opts.LogPath
	os.MkdirAll(path.Dir(opts.LogPath), 0777)

	outlogPath := logPath + ".log"
	errLogPath := logPath + ".log.err"
	slowLogPath := logPath + ".log.slow"

	l := &Logger{}
	l.debug = opts.IsDebug
	l.outWriter = getWriter(outlogPath, opts.RotationTime)
	l.errWriter = getWriter(errLogPath, opts.RotationTime)
	l.slowWriter = getWriter(slowLogPath, opts.RotationTime)
	l.outLogger = zap.New(zapcore.NewCore(zapcore.NewConsoleEncoder(zapcore.EncoderConfig{MessageKey: "out"}), l.outWriter, &LevelEnable{}))
	l.errLogger = zap.New(zapcore.NewCore(zapcore.NewConsoleEncoder(zapcore.EncoderConfig{MessageKey: "err"}), l.errWriter, &LevelEnable{}))
	l.slowLogger = zap.New(zapcore.NewCore(zapcore.NewConsoleEncoder(zapcore.EncoderConfig{MessageKey: "slow"}), l.slowWriter, &LevelEnable{}))
	log = l
	return l
}

func GetLogger() *Logger {
	return log
}

func getWriter(path, rotation string) zapcore.WriteSyncer {
	os.Remove(path)
	hook := getRotateLogs(path, rotation)
	return zapcore.AddSync(hook)
}

func now() time.Time {
	return time.Now()
}

func FileLine(caller interface{}, length int) string {
	var p uintptr
	switch caller := caller.(type) {
	case nil:
		p, _, _, _ = runtime.Caller(2)
	case int:
		p, _, _, _ = runtime.Caller(caller)
	default:
		p = reflect.ValueOf(caller).Pointer()
	}

	f := runtime.FuncForPC(p)
	file, line := f.FileLine(p)
	ls := strings.Split(file, "/")
	if len(ls) > length {
		ls = ls[len(ls)-length:]
	}
	return fmt.Sprintf("%s:%d", strings.Join(ls, "/"), line)
}
