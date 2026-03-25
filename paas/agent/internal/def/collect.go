package def

import "time"

const (
	ServerLogType = 1
	ProxyLogType  = 2

	ServerSlowLogType   = 1
	ServerErrLogType    = 2
	ProxySlowLogType    = 3
	ProxyErrLogType     = 4
	ServerActionLogType = 5

	BitupleFileType = 6

	AntsNums    = 50
	HttpTimeout = 10 * time.Second
)

const (
	ActionTypeBdbMemtableFlushed  = 1
	ActionTypeBdbCompacted        = 2
	ActionTypeDelete              = 3
	ActionTypeBdbMemtableFlushing = 4
	ActionTypeBdbCompacting       = 5
	ActionTypeBitableMemFlushing  = 6 //expiredb
	ActionTypeBitableMemFlushed   = 7
	ActionTypeBitableCompacting   = 8
	ActionTypeBitableMemCompacted = 9
	ActionTypeRaftFlushed         = 10
	ActionTypeRaftCompacted       = 11

	ActionTypeBitupleVti = 12
	ActionTypeBitupleVtk = 13
	ActionTypeBitupleVtm = 14
	ActionTypeBitupleVtv = 15

	ActionTypeVmFlushed = 16
	ActionTypeVtGC      = 17
	ActionTypeV8Delete  = 18
	ActionTypeVtRehash  = 19
)

const (
	ActionStatusStart uint8 = iota
	ActionStatusDoing
	ActionStatusEnd
)

const (
	LogLevelPanic uint8 = iota
	LogLevelError
	LogLevelWarn
	LogLevelInfo
)

const (
	ExceptionTimeout       = "i/o timeout"
	ExceptionRefuseConnect = "connection refused"
	ExceptionRaftConnect   = "raft connect failed"
	ExceptionMasterChange  = "master change"
)

func TransferProxyLogLevel(logLevel string) uint8 {
	switch logLevel {
	case "[PANIC]":
		return LogLevelPanic
	case "[ERROR]":
		return LogLevelError
	case "[WARN]":
		return LogLevelWarn
	}
	return LogLevelWarn
}
