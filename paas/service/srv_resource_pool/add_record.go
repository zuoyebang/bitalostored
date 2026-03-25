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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
)

type AddResourceRecordInput struct {
	ClusterId   int    `json:"clusterId"`
	ClusterName string `json:"clusterName"`
	ServiceType string `json:"serviceType"`
	Idc         string `json:"idc"`
	Cpu         int    `json:"cpu"`
}

func (input *AddResourceRecordInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("empty params")
	}
	return nil
}

func (input *AddResourceRecordInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	serviceId := uint(0)
	if input.ServiceType == "proxy" {
		serviceId = def.SERVICE_ID_PROXY
	} else if input.ServiceType == "server" {
		serviceId = def.SERVICE_ID_BITALOS
	} else {
		return nil, errors.New("serviceType error")
	}
	port := uint(0)
	if serviceId == def.SERVICE_ID_PROXY {
		node, err := tbl_node.GetOneByClusterId(uint(input.ClusterId))
		if err != nil {
			return nil, err
		}
		port = node.ServicePort
	}
	_, err := tbl_resource_pool.Create(input.ClusterName, def.CGROUP_NAME_CPU, input.Idc, uint(input.ClusterId), serviceId, port, int64(input.Cpu))
	if err != nil {
		return nil, err
	}
	return nil, nil
}
