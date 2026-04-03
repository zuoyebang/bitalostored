package http

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/response"
	"github.com/zuoyebang/bitalostored/paas/agent/service"
)

func GetCgroups(ctx *gin.Context) {
	res := service.GetCgroups()
	response.RenderJsonSucc(ctx, res)
}
