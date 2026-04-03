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

package srv_region

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_regionmachine"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
)

type BindMachinesInput struct {
	RegionId   uint   `json:"regionId"`
	MachineId  uint   `json:"machineId"`
	MachineIds []uint `json:"machineIds"`
}

var _ servicer.Servicer = new(BindMachinesInput)

func (input *BindMachinesInput) CheckParams(ctx *gin.Context) error {
	if input.RegionId <= 0 {
		return errors.New("invalid regionId")
	}
	if input.MachineId <= 0 && len(input.MachineIds) == 0 {
		return errors.New("invalid machine")
	}
	return nil
}
func (input *BindMachinesInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	if len(input.MachineIds) == 0 {
		input.MachineIds = append(input.MachineIds, input.MachineId)
	}
	return tbl_regionmachine.Create(input.MachineIds, []uint{input.RegionId}), nil
}
