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

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	TypeInfo  = "INFO"
	TypeWarn  = "WARN"
	TypeError = "ERROR"
	TypeFatal = "FATAL"
	TypeDebug = "DEBUG"
)

const (
	headerFormat = "%s.%06d %s [%s] "
	accessFormat = "[ip_port:%s] [duration(us):%d] [status:%s] [query:%q] "
	slowFormat   = "[ip_port:%s] [duration(us):%d] [status:%s] [query:%q] "
)

type Logger struct {
	debug        bool
	outWriter    zapcore.WriteSyncer
	statsWriter  zapcore.WriteSyncer
	accessWriter zapcore.WriteSyncer
	slowWriter   zapcore.WriteSyncer
	outLogger    *zap.Logger
	statsLogger  *zap.Logger
	accessLogger *zap.Logger
	slowLogger   *zap.Logger
}

func (l *Logger) CloseSync() {
	if l.outLogger != nil {
		l.outLogger.Sync()
	}
	if l.accessLogger != nil {
		l.accessLogger.Sync()
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
	now := time.Now()
	fileLine := FileLine(5, 2)
	return fmt.Sprintf(headerFormat, now.Format(time.DateTime), int64(now.Nanosecond()/1000), fileLine, level)
}

func (l *Logger) output(level string, arg ...interface{}) {
	if l.outLogger == nil || l.filter(level) {
		return
	}
	l.outLogger.Info(fmt.Sprint(l.formatHeader(level), fmt.Sprint(arg...)))
}

func (l *Logger) outputf(level string, format string, arg ...interface{}) {
	if l.outLogger == nil || l.filter(level) {
		return
	}
	l.outLogger.Info(fmt.Sprint(l.formatHeader(level), fmt.Sprintf(format, arg...)))
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

func (l *Logger) Stats(arg interface{}) {
	if l.statsLogger == nil {
		return
	}

	l.statsLogger.Info(fmt.Sprint(l.formatHeader(TypeInfo), arg))
}

func (l *Logger) Access(remoteAddr string, cost int64, request []byte, err error) {
	if l.accessLogger == nil {
		return
	}

	var status string
	if err == nil {
		status = "OK"
	} else {
		status = err.Error()
	}

	s := fmt.Sprintf(accessFormat, remoteAddr, cost, status, request)
	l.accessLogger.Info(fmt.Sprint(l.formatHeader(TypeInfo), s))
}

func (l *Logger) Slow(remoteAddr string, cost int64, request []byte, err error) {
	if l.slowLogger == nil {
		return
	}

	var status string
	if err == nil {
		status = "OK"
	} else {
		status = err.Error()
	}

	s := fmt.Sprintf(slowFormat, remoteAddr, cost, status, request)
	l.slowLogger.Info(fmt.Sprint(l.formatHeader(TypeInfo), s))
}

type LevelEnable struct{}

func (le *LevelEnable) Enabled(l zapcore.Level) bool {
	return true
}

type Options struct {
	IsDebug       bool
	RotationTime  string
	LogFile       string
	StatsLogFile  string
	SlowLog       bool
	SlowLogFile   string
	AccessLog     bool
	AccessLogFile string
}

func NewLogger(opts *Options) *Logger {
	os.MkdirAll(path.Dir(opts.LogFile), 0777)
	l := &Logger{}
	l.debug = opts.IsDebug
	l.outWriter = getWriter(opts.LogFile, opts.RotationTime)
	l.outLogger = zap.New(zapcore.NewCore(zapcore.NewConsoleEncoder(zapcore.EncoderConfig{MessageKey: "out"}), l.outWriter, &LevelEnable{}))
	l.statsWriter = getWriter(opts.StatsLogFile, opts.RotationTime)
	l.statsLogger = zap.New(zapcore.NewCore(zapcore.NewConsoleEncoder(zapcore.EncoderConfig{MessageKey: "stats"}), l.statsWriter, &LevelEnable{}))

	if opts.AccessLog {
		l.accessWriter = getWriter(opts.AccessLogFile, opts.RotationTime)
		l.accessLogger = zap.New(zapcore.NewCore(zapcore.NewConsoleEncoder(zapcore.EncoderConfig{MessageKey: "access"}), l.accessWriter, &LevelEnable{}))
	}

	if opts.SlowLog {
		l.slowWriter = getWriter(opts.SlowLogFile, opts.RotationTime)
		l.slowLogger = zap.New(zapcore.NewCore(zapcore.NewConsoleEncoder(zapcore.EncoderConfig{MessageKey: "slow"}), l.slowWriter, &LevelEnable{}))
	}

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
