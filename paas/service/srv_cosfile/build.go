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

package srv_cosfile

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cosfile"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"io"
	"net/http"
	"strings"
)

type BuildFileInput struct {
	ServiceId uint   `json:"serviceId"`
	GitBranch string `json:"gitBranch"`
	Server    string `json:"server"`
}

var _ servicer.Servicer = new(BuildFileInput)

func (input *BuildFileInput) CheckParams(ctx *gin.Context) error {
	if input.GitBranch == "" {
		return errors.New("invalid gitBranch")
	}
	if input.ServiceId == 0 {
		return errors.New("invalid serviceId")
	}
	return nil
}
func (input *BuildFileInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	if input.ServiceId == def.SERVICE_ID_MATRIX || input.ServiceId == def.SERVICE_ID_BITALOS {
		input.ServiceId = def.SERVICE_ID_BITALOS
	}
	serviceInfo, err := tbl_service.GetInfo(input.ServiceId)
	if err != nil {
		log.Warnf("get service info failed.err:%+v", err)
		return nil, err
	}
	resp, err := http.Get(config.GetConf().BuildServer.Address + fmt.Sprintf("/build?gitModule=%s&gitBranch=%s", serviceInfo.Name, input.GitBranch))
	if err != nil {
		log.Warnf("build file failed.err:%+v", err)
		return nil, err
	}
	body, _ := io.ReadAll(resp.Body)
	builderResp := BuilderResp{}
	err = json.Unmarshal(body, &builderResp)
	if err != nil {
		log.Warnf("build file failed.Wrong data.err:%+v", err)
		return nil, err
	}
	if builderResp.ErrNo != 0 {
		log.Warnf("builder exec failed.err:%v", builderResp.ErrStr)
		return nil, errors.New(builderResp.ErrStr)
	}
	if serviceInfo.Name == def.SERVICE_STORED_FE {
		hashes := strings.Split(builderResp.Data.Hash, " ")
		if len(hashes) != 2 {
			return nil, errors.New("build service mismatch")
		}
		fileName := def.GetBinNameByServiceName(def.SERVICE_STORED_FE)
		_, err = tbl_cosfile.Create(fileName, fileName+"_"+input.GitBranch+"_"+hashes[0], def.FILE_TYPE_MAIN, def.FILE_MODE_MAIN, hashes[0], input.GitBranch, input.ServiceId)
		if err != nil {
			log.Warnf("create file failed.insert data failed.err:%+v", err)
			return nil, err
		}
		fileName = def.GetBinNameByServiceName(def.SERVICE_STORED_FE_ZIP)
		_, err = tbl_cosfile.Create(fileName, fileName+"_"+input.GitBranch+"_"+hashes[1], def.FILE_TYPE_COMPRESS, def.FILE_MODE_CONF, hashes[1], input.GitBranch, input.ServiceId)
		if err != nil {
			log.Warnf("create file failed.insert data failed.err:%+v", err)
			return nil, err
		}
		return builderResp.Data.ScriptResult, nil
	}
	fileName := def.GetBinNameByServiceName(serviceInfo.Name)
	_, err = tbl_cosfile.Create(fileName, fileName+"_"+input.GitBranch+"_"+builderResp.Data.Hash, def.FILE_TYPE_MAIN, def.FILE_MODE_MAIN, builderResp.Data.Hash, input.GitBranch, input.ServiceId)
	if err != nil {
		log.Warnf("create file failed.insert data failed.err:%+v", err)
		return nil, err
	}
	return builderResp.Data.ScriptResult, nil
}

type BuilderResp struct {
	ErrNo  int             `json:"errNo"`
	ErrStr string          `json:"errStr"`
	Data   BuilderRespData `json:"data"`
}

type BuilderRespData struct {
	Hash         string `json:"hash"`
	ScriptResult string `json:"scriptResult"`
}
