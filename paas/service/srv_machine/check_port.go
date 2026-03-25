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

package srv_machine

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_hostport"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_regionmachine"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type CheckPortInput struct {
	RegionId uint `form:"regionId"`
	Port     uint `form:"port"`
}

var _ servicer.Servicer = new(CheckPortInput)

func (input *CheckPortInput) CheckParams(ctx *gin.Context) error {
	if input.RegionId <= 0 {
		return errors.New("invalid regionId")
	}
	if input.Port <= 0 || input.Port > 65535 {
		return errors.New("invalid port")
	}
	return nil
}
func (input *CheckPortInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	machineIdList, err := tbl_regionmachine.GetMachinesByRegion(input.RegionId)
	if err != nil {
		log.Warn("get machine list failed.err:", err)
		return nil, err
	}
	for _, mId := range machineIdList {
		if tbl_hostport.IsExist(mId, input.Port) {
			return CheckPortResp{false}, nil
		}
	}
	return CheckPortResp{true}, nil
}

type CheckPortResp struct {
	IsLegal bool `json:"isLegal"`
}
