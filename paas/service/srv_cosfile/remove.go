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
	"fmt"
	"github.com/gin-gonic/gin"
	jsoniter "github.com/json-iterator/go"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cosfile"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"io"
	"net/http"
)

type FileRemoveInput struct {
	FileId uint `json:"id"`
}

var _ servicer.Servicer = new(FileListInput)

func (input *FileRemoveInput) CheckParams(ctx *gin.Context) error {
	if input.FileId == 0 {
		return errors.New("invalid id")
	}
	return nil
}

func (input *FileRemoveInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	fileInfo, err := tbl_cosfile.GetCosFile(input.FileId)
	if err != nil {
		log.Warn("get cos file failed.err:", err)
		return nil, err
	}
	if tbl_node.IsFileInUse(fileInfo.ServiceId, input.FileId) {
		return nil, errors.New("the file is still on use. Could not remove it.")
	}
	if fileInfo.FileType != "lan" {
		resp, err := http.Get(config.GetConf().BuildServer.Address + fmt.Sprintf("/remove?cosFile=%s", fileInfo.CosKey))
		if err != nil {
			log.Warnf("build file failed.err:%+v", err)
			return nil, err
		}
		body, _ := io.ReadAll(resp.Body)
		builderResp := RemoveResp{}
		err = jsoniter.Unmarshal(body, &builderResp)
		if err != nil {
			log.Warnf("remove file failed.Wrong data.err:%+v", err)
			return nil, err
		}
		if builderResp.ErrNo != 0 {
			log.Warnf("remove exec failed.err:%v", builderResp.ErrStr)
			return nil, errors.New(builderResp.ErrStr)
		}
	}
	return nil, tbl_cosfile.DeleteFile(input.FileId)
}

type RemoveResp struct {
	ErrNo  int    `json:"errNo"`
	ErrStr string `json:"errStr"`
	Data   string `json:"data"`
}
