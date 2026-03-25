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

package srv_service

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type ServiceOperationsInput struct {
	ServiceId uint `form:"serviceId"`
}

func (input *ServiceOperationsInput) CheckParams(ctx *gin.Context) error {
	if input.ServiceId <= 0 {
		return errors.New("invalid serviceId")
	}
	return nil
}
func (input *ServiceOperationsInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	var output ServiceOpterationsOutput
	var err error
	info, err := tbl_service.GetInfo(input.ServiceId)
	if err != nil {
		log.Warn("get service info failed")
		output.Operations = def.MatrixOperationList
		return output, nil
	}
	switch info.Name {
	case def.SERVICE_MATRIX:
		output.Operations = def.MatrixOperationList
	case def.SERVICE_BITALOS:
		output.Operations = def.BitalosOperationList
	case def.SERVICE_STORED_PROXY:
		output.Operations = def.ProxyOperationList
	case def.SERVICE_STORED_DASHBOARD:
		output.Operations = def.DashboardOperationList
	case def.SERVICE_STORED_FE:
		output.Operations = def.FEOperationList
	}
	return output, nil
}

type ServiceOpterationsOutput struct {
	Operations []string `json:"operations"`
}
