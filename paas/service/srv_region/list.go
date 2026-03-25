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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_regionmachine"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_region"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type RegionListInput struct {
	ServiceId uint `form:"serviceId"`
}

func (input *RegionListInput) CheckParams(ctx *gin.Context) error {
	return nil
}
func (input *RegionListInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	var output RegionListOutput
	regionList, err := tbl_region.GetList(-1, 0)
	if err != nil {
		log.Errorf("get region list failed.err:%+v", err)
		return nil, err
	}
	if len(regionList) == 0 {
		output.Rows = []RegionList{}
		return output, nil
	}
	//get region ids
	regionIds := make(map[uint]uint, 0)
	for _, region := range regionList {
		regionIds[region.ID] = 1
	}
	serviceId := input.ServiceId
	if serviceId == def.SERVICE_ID_AGENT {
		serviceId = 0
	}
	for _, region := range regionList {
		if region.NewId > 0 {
			continue
		}
		count := tbl_cluster.Count(region.ID, serviceId, def.CLUSTER_STATUS_ONLINE)
		if count > 0 && serviceId == 0 {
			matrix, proxy := mdl_region.GetRegionClusters(region.ID)
			output.Rows = append(output.Rows, RegionList{
				Region:     region,
				Matrix:     matrix,
				Proxy:      proxy,
				MachineSum: tbl_regionmachine.GetRegionMachineCount(region.ID),
			})
		} else {
			output.Rows = append(output.Rows, RegionList{
				Region: region,
			})
		}
	}
	output.Count = len(output.Rows)
	return output, err
}

type RegionListOutput struct {
	Rows  []RegionList `json:"rows"`
	Count int          `json:"count"`
}

type RegionList struct {
	*tbl_region.Region
	Matrix     []string `json:"matrix"`
	Proxy      []string `json:"proxy"`
	MachineSum int64    `json:"machineSum"`
}
