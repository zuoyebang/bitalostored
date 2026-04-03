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

package srv_cluster

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type StoredListInput struct {
	IsDashboard bool `form:"isDashboard"`
}

var _ servicer.Servicer = new(StoredListInput)

func (input *StoredListInput) CheckParams(ctx *gin.Context) error {
	return nil
}

func (input *StoredListInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	var output StoredListOutput
	var serviceInfo *tbl_service.Service
	serviceInfo, _ = tbl_service.GetInfoByName(def.SERVICE_STORED_DASHBOARD)
	dashboardService := serviceInfo.ID
	if !input.IsDashboard {
		serviceInfo, _ = tbl_service.GetInfoByName(def.SERVICE_STORED_FE)
	}

	clusterList, err := tbl_cluster.GetList(0, serviceInfo.ID, "")
	if err != nil {
		log.Errorf("get cluster list failed.err:%+v", err)
		return nil, err
	}
	for _, cluster := range clusterList {
		nodes, _ := tbl_node.GetListByCluster(cluster.Id)
		if len(nodes) != 1 {
			continue
		}
		machineInfo, _ := tbl_machine.GetInfo(nodes[0].MachineId)
		address := ""
		if !input.IsDashboard {
			tbl_cluster.SameStoredCluster(cluster.StoredId)
			address = fmt.Sprintf("http://%s:%d/#/%s", machineInfo.IP, nodes[0].ServicePort, getDashboardName(cluster.StoredId, dashboardService))
		}
		output.StoredList = append(output.StoredList, StoredInfo{
			Node:        nodes[0],
			ClusterName: cluster.Name,
			StoredId:    cluster.StoredId,
			IP:          machineInfo.IP,
			JumpAddress: address,
		})
	}
	output.ServiceId = serviceInfo.ID
	return output, nil
}

func getDashboardName(storedId, serviceId uint) string {
	clusterList, err := tbl_cluster.SameStoredCluster(storedId)
	if err != nil {
		return ""
	}
	for _, cluster := range clusterList {
		if cluster.ServiceId == serviceId {
			return cluster.Name
		}
	}
	return ""
}
