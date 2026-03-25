package logs

import (
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Field = zapcore.Field

func String(key, value string) Field {
	return zap.String(key, value)
}

func GetLogID(c *gin.Context) string {
	if c == nil {
		return ""
	}
	logID, exists := c.Get("logId")
	if !exists {
		return ""
	}
	if id, ok := logID.(string); ok {
		return id
	}
	return ""
}

func GetRequestID(c *gin.Context) string {
	if c == nil {
		return ""
	}
	requestID, exists := c.Get("requestId")
	if !exists {
		return ""
	}
	if id, ok := requestID.(string); ok {
		return id
	}
	return ""
}

func InfoLogger(c *gin.Context, msg string, fields ...Field) {
	Infof("%s %v", msg, fields)
}
