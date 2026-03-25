package response

import (
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/errors"
	"net/http"

	"github.com/gin-gonic/gin"
)

func RenderJsonSucc(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, gin.H{
		"errno":  0,
		"errmsg": "success",
		"data":   data,
	})
}

func RenderJsonFail(c *gin.Context, err error) {
	if e, ok := err.(errors.Error); ok {
		c.JSON(http.StatusOK, gin.H{
			"errno":  e.ErrNo,
			"errmsg": e.ErrMsg,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"errno":  5000,
		"errmsg": err.Error(),
	})
}

func RenderJsonAbort(c *gin.Context, err error) {
	RenderJsonFail(c, err)
	c.Abort()
}
