package http

import (
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/response"
	"github.com/zuoyebang/bitalostored/paas/agent/service"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
)

type LogQueryReq struct {
	Address   string `json:"address" form:"address" binding:"required"`
	Query     string `json:"query" form:"query"`
	QueryTime int64  `json:"queryTime" form:"queryTime" binding:"required"`
}

func LogQuery(ctx *gin.Context) {
	req := &LogQueryReq{}
	ctx.Header("Access-Control-Allow-Origin", "*")
	ctx.Header("Access-Control-Allow-Methods", "*")
	ctx.Header("Access-Control-Allow-Headers", "Content-Type")
	if ctx.Request.Method == "OPTIONS" {
		response.RenderJsonSucc(ctx, nil)
		return
	}
	if err := ctx.ShouldBindWith(req, binding.JSON); err != nil {
		response.RenderJsonFail(ctx, errors.ErrorParamInvalid)
		return
	}
	if len(req.Address) <= 0 || req.QueryTime <= 0 {
		response.RenderJsonFail(ctx, errors.ErrorParamInvalid)
		return
	}
	addressSp := strings.Split(req.Address, ":")
	if len(addressSp) != 2 {
		response.RenderJsonFail(ctx, errors.ErrorParamInvalid)
		return
	}
	res, resNum, err := service.LogQuery(ctx, addressSp[1], req.Query, req.QueryTime)
	if err != nil {
		response.RenderJsonFail(ctx, err)
		return
	}
	var num string
	if resNum == nil {
		num = "0"
	} else {
		num = string(resNum)
	}
	ret := "Log count (up to 1000): " + num + "\n" + string(res)
	response.RenderJsonSucc(ctx, ret)
}
