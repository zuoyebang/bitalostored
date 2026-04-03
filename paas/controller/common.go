// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"bytes"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/service/srv_user"
	"github.com/zuoyebang/bitalostored/paas/utils"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	jsoniter "github.com/json-iterator/go"
	"github.com/zuoyebang/bitalostored/paas/utils/unsafe2"
)

func process(ctx *gin.Context, srv servicer.Servicer) {
	ctx.Header("Access-Control-Allow-Origin", "*")
	ctx.Header("Access-Control-Allow-Headers", "Content-Type,AccessToken,X-CSRF-Token, Authorization, Token, Cookie")
	ctx.Header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	ctx.Header("Access-Control-Expose-Headers", "Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers, Content-Type")
	ctx.Header("Access-Control-Allow-Credentials", "true")

	rawBody, err := ctx.GetRawData()
	ctx.Request.Body = io.NopCloser(bytes.NewBuffer(rawBody))
	if errParams := ctx.ShouldBindJSON(&srv); errParams != nil {
		if errParams == io.EOF {
			outputError(ctx, def.PARAM_ERROR, def.ErrMsg[def.PARAM_ERROR])
		} else {
			outputError(ctx, def.PARAM_ERROR, errParams.Error())
		}
		log.Warn("params:", errParams)
		return
	}
	var uname string
	if !isSkipVerifyURL(ctx.Request) {
		if !checkPtoken(ctx) {
			return
		}
		recordQueue(ctx, uname, rawBody)
	}

	log.Infof("access-log[%+v]  user[%s] request param[%+v]", ctx.FullPath(), uname, srv)
	if err := srv.CheckParams(ctx); err != nil {
		outputError(ctx, def.PARAM_ERROR, err.Error())
		return
	}
	data, err := srv.BuildOutput(ctx)
	if err != nil {
		outputError(ctx, def.UNKNOWN_ERROR, err.Error())
		return
	}
	outputData(ctx, data)
}

func processGet(ctx *gin.Context, srv servicer.Servicer) {
	ctx.Header("Access-Control-Allow-Origin", "*")
	ctx.Header("Access-Control-Allow-Headers", "Content-Type,AccessToken,X-CSRF-Token, Authorization, Token, Cookie")
	ctx.Header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	ctx.Header("Access-Control-Expose-Headers", "Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers, Content-Type")
	ctx.Header("Access-Control-Allow-Credentials", "true")
	if err := ctx.ShouldBind(srv); err != nil {
		outputError(ctx, def.PARAM_ERROR, err.Error())
		log.Warn("params:", err)
		return
	}
	if err := srv.CheckParams(ctx); err != nil {
		outputError(ctx, def.PARAM_ERROR, err.Error())
		return
	}
	if !isSkipVerifyURL(ctx.Request) {
		if !checkPtoken(ctx) {
			return
		}
	}
	data, err := srv.BuildOutput(ctx)
	if err != nil {
		outputError(ctx, def.UNKNOWN_ERROR, err.Error())
		return
	}
	outputData(ctx, data)
}

func checkPtoken(ctx *gin.Context) bool {
	token, err := ctx.Cookie(def.CookiePToken)
	if err != nil {
		outputError(ctx, def.TOKEN_ERROR, "missing ptoken.need login")
		return false
	}
	if !srv_user.VerifyPToken(token) {
		outputError(ctx, def.TOKEN_ERROR, "illegal ptoken")
		return false
	}
	return true
}

func recordQueue(ctx *gin.Context, uname string, rawBody []byte) {
	qdata := &utils.QData{
		Url:      ctx.Request.URL.Path,
		Username: uname,
	}
	if len(rawBody) > 256 {
		qdata.OpData = string(rawBody[:256])
	} else {
		qdata.OpData = string(rawBody)
	}
	q, _ := ctx.Get(utils.QueueOperation)
	queue, ok := q.(*utils.Queue)
	if !ok {
		log.Warnf("not insert db  qdata=%+v", qdata)
	} else {
		_ = queue.Push(qdata)
	}
}

func outputError(ctx *gin.Context, errNo int, errStr string) {
	body := gin.H{"status": errNo, "msg": errStr, "data": map[string]interface{}{}}
	data, _ := jsoniter.Marshal(body)
	ctx.String(200, unsafe2.String(data))
}

type UserInfo struct {
	Uname       string `json:"uname"`
	Username    string `json:"username"`
	Email       string `json:"email"`
	LastLogTime int64  `json:"lastLogTime"`
	LoginFrom   string `json:"loginFrom"`
	Uid         int64  `json:"uid"`
}

func outputData(ctx *gin.Context, data interface{}) {
	response := gin.H{"status": 0, "msg": "ok", "data": data}
	body, err := jsoniter.Marshal(response)
	if err != nil {
		log.Errorf("marshal response data error:%+v", err)
		ctx.String(200, `{"status":20000,"msg":"marshal data error","data":{}}`)
	}
	if !isSkipOutputURL(ctx.FullPath()) {
		log.Infof("fullPath:%s response:%s", ctx.FullPath(), unsafe2.String(body))
	}
	ctx.String(200, unsafe2.String(body))
}

func isSkipOutputURL(url string) bool {
	switch url {
	case "/bitalospaas/task/list":
		return true
	case "/bitalospaas/agent/manageinfo":
		return true
	case "/bitalospaas/machine/clusterinfo":
		return true
	default:
		return false
	}
}

func isSkipVerifyURL(req *http.Request) bool {
	switch req.URL.Path {
	case "/bitalospaas/login":
		return true
	case "/bitalospaas/resource/upload":
		return true
	case "/bitalospaas/package/create":
		return true
	case "/bitalospaas/machine/register":
		return true
	case "/bitalospaas/task/list":
		return true
	case "/bitalospaas/task/upgraded":
		return true
	case "/bitalospaas/task/status":
		return true
	case "/bitalospaas/task/hostport":
		return true
	case "/bitalospaas/task/prepared":
		return true
	case "/bitalospaas/agent/manageinfo":
		return true
	case "/bitalospaas/agent/updated":
		return true
	case "/bitalospaas/machine/recovery":
		return true
	case "/bitalospaas/machine/nodedeployinfo":
		return true
	case "/bitalospaas/cluster/clusternames":
		return true
	case "/bitalospaas/service/list", "/bitalospaas/controlfe/formfields", "/bitalospaas/controlfe/constantlist":
		return true
	case "/bitalospaas/file/download":
		return true
	default:
		return false
	}
}
