package middleware

import (
	"bytes"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/response"
	"io/ioutil"

	"github.com/gin-gonic/gin"
)

func Recover(ctx *gin.Context) {
	defer CatchRecoverRpc(ctx)
	ctx.Next()
}

func CatchRecoverRpc(c *gin.Context) {
	if err := recover(); err != nil {
		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery
		if raw != "" {
			path = path + "?" + raw
		}
		body, _ := ioutil.ReadAll(c.Request.Body)
		c.Request.Body = ioutil.NopCloser(bytes.NewBuffer(body))

		logs.Infof("Panic[recover] logId=%s requestId=%s uri=%s refer=%s clientIp=%s module=bitalosagent ua=%s host=%s err=%s",
			logs.GetLogID(c),
			logs.GetRequestID(c),
			path,
			c.Request.Referer(),
			c.ClientIP(),
			"bitalosagent",
			c.Request.UserAgent(),
			c.Request.Host,
			err.(string),
		)

		response.RenderJsonAbort(c, errors.ErrorSystemError)
	}
}
