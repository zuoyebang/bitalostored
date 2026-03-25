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

package srv_config

import (
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_config"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
)

type UpdateConfigInput struct {
	Action  string                  `json:"action"`
	Configs []*tbl_config.TblConfig `json:"configs"`
}

var _ servicer.Servicer = new(UpdateConfigInput)

func (input *UpdateConfigInput) CheckParams(ctx *gin.Context) error {
	if len(input.Configs) == 0 {
		return errors.New("invalid configs")
	}
	return nil
}

func (input *UpdateConfigInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	if input.Action == "new" || input.Action == "copy" {
		id, err := tbl_config.CreateConfigs(input.Configs[0].ServiceId, input.Configs)
		return UpdateResp{id}, err
	}
	if input.Action == "update" {
		id, err := tbl_config.UpdateConfigs(input.Configs)
		return UpdateResp{id}, err
	}
	return nil, errors.New("support new,copy,update action only")
}

type UpdateResp struct {
	ConfigPackId uint `json:"configPackId"`
}
