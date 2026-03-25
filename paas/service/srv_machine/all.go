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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"

	"github.com/gin-gonic/gin"
)

type MachineAllInput struct {
	RegionId uint `form:"regionId"`
}

var _ servicer.Servicer = new(MachineAllInput)

func (input *MachineAllInput) CheckParams(ctx *gin.Context) error {
	return nil
}
func (input *MachineAllInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	var output MachineAllOutput
	var err error
	// if input.RegionId > 0 {
	// 	regionId := input.RegionId
	// 	//get new regionId
	// 	r, err := tbl_region.GetInfo(regionId)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	if r.NewId > 0 {
	// 		regionId = r.NewId
	// 	}
	// 	output.Rows = mdl_machine.GetMachinesByRegion(regionId, []string{def.MACHINE_STATUS_ONLINE})
	// } else {
	output.Rows, err = tbl_machine.GetOnlineMachines()
	// }
	return output, err
}

type MachineAllOutput struct {
	Rows []*tbl_machine.Machine `json:"rows"`
}
