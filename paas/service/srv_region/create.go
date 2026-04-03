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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_regionmachine"
)

type CreateRegionInput struct {
	RegionName string `json:"regionName"`
	MachineIds []uint `json:"machineIds"`
}

func (input *CreateRegionInput) CheckParams(ctx *gin.Context) error {
	return nil
}
func (input *CreateRegionInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	res, err := tbl_region.Create(input.RegionName)
	if err != nil {
		return nil, err
	}
	return &CreateRegionOutput{RegionId: res.ID, BindMachines: tbl_regionmachine.Create(input.MachineIds, []uint{res.ID})}, nil
}

type CreateRegionOutput struct {
	RegionId     uint `json:"regionId"`
	BindMachines int  `json:"bindMachines"`
}
