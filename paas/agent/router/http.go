package router

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/agent/controllers/http"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/middleware"
)

func Http(engine *gin.Engine) {
	router := engine.Group("bitalosagent")
	router.Use(middleware.Recover)

	router.OPTIONS("/logquery", http.LogQuery)
	router.POST("/logquery", http.LogQuery)
	router.GET("/getcgroup", http.GetCgroups)
}
