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

package srv_resource_pool

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type ListInput struct {
	ServiceId   int    `json:"serviceId"`
	ClusterName string `json:"clusterName"`
	IDC         string `json:"idc"`
	IsManual    int    `json:"isManual"`
	CpuSetType  int    `json:"cpuSetType"`
}

func (input *ListInput) CheckParams(ctx *gin.Context) error {
	return nil
}

func (input *ListInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	var output RegionListOutput
	clusterName := input.ClusterName
	if input.ClusterName == "our-search-page" {
		clusterName = "ocr-search-page"
	}
	if input.ClusterName == "our-search-inv" {
		clusterName = "ocr-search-inv"
	}
	resourceList, err := tbl_resource_pool.GetResourceList(clusterName, input.IDC, input.ServiceId, input.IsManual, input.CpuSetType)
	if err != nil {
		log.Errorf("get resource list failed.err:%+v", err)
		return nil, err
	}
	if len(resourceList) == 0 {
		output.Rows = []ResourceList{}
		return output, nil
	}
	var serviceName string
	for _, region := range resourceList {
		if region.ServiceId == def.SERVICE_ID_PROXY {
			serviceName = "proxy"
		}
		if region.ServiceId == def.SERVICE_ID_BITALOS {
			serviceName = "server"
		}

		output.Rows = append(output.Rows, ResourceList{
			region,
			serviceName,
		})
	}
	output.Count = len(output.Rows)
	return output, err
}

type RegionListOutput struct {
	Rows  []ResourceList `json:"rows"`
	Count int            `json:"count"`
}

type ResourceList struct {
	*tbl_resource_pool.Resource
	ServiceName string `json:"serviceName"`
}
