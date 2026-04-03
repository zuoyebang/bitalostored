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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_group"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strings"
)

type ClusterIPListInput struct {
	ClusterId   uint   `form:"clusterId"`
	ClusterName string `form:"clusterName"`
}

var _ servicer.Servicer = new(ClusterIPListInput)

func (input *ClusterIPListInput) CheckParams(ctx *gin.Context) error {
	fmt.Println(input.ClusterId)
	if input.ClusterId == 0 && strings.TrimSpace(input.ClusterName) == "" {
		return errors.New("missing clusterId or clusterName")
	}
	return nil
}

func (input *ClusterIPListInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	if input.ClusterId == 0 {
		serviceInfo, err := tbl_service.GetInfoByName(def.SERVICE_MATRIX)
		if err != nil {
			log.Warn("get serviceId by name failed.")
			return "", err
		}
		clusterInfo, err := tbl_cluster.GetInfoByName(input.ClusterName, serviceInfo.ID)
		if err != nil {
			log.Warnf("failed to get clusters.err:%+v", err)
			return "", err
		}
		input.ClusterId = clusterInfo.Id
	}

	groupList, err := tbl_group.GetList(input.ClusterId, "", -1, 0)
	if err != nil {
		log.Warn("get group list failed.")
		return "", err
	}
	tencentExport, baiduExport, aliExport, txCloudExport := "", "", "", ""
	for _, group := range groupList {
		nodeInfos, err := tbl_node.GetOnlineListByGroup(input.ClusterId, group.GroupId, -1, 0)
		if err != nil {
			log.Warn("get node list failed.")
			return "", err
		}
		for _, node := range nodeInfos {
			witnessTag := ""
			if node.IsWitness {
				witnessTag = "-witness"
			}
			machineInfo, err := tbl_machine.GetInfo(node.MachineId)
			if err != nil {
				log.Warn("get machine info failed.")
				return "", err
			}
			if machineInfo.IDC == def.IdcBaidu {
				baiduExport = fmt.Sprintf("%s%s:%d,,group%d%s\n", baiduExport, machineInfo.IP, node.ServicePort, group.GroupId, witnessTag)
				continue
			}
			if machineInfo.IDC == def.IdcTencent {
				tencentExport = fmt.Sprintf("%s%s:%d,,group%d%s\n", tencentExport, machineInfo.IP, node.ServicePort, group.GroupId, witnessTag)
				continue
			}
			if machineInfo.IDC == def.IdcAli {
				aliExport = fmt.Sprintf("%s%s:%d,,group%d%s\n", aliExport, machineInfo.IP, node.ServicePort, group.GroupId, witnessTag)
				continue
			}
			if machineInfo.IDC == def.IdcTxcloud {
				txCloudExport = fmt.Sprintf("%s%s:%d,,group%d%s\n", txCloudExport, machineInfo.IP, node.ServicePort, group.GroupId, witnessTag)
				continue
			}
		}
	}

	return fmt.Sprintf("baidu:\n%s\ntencent:\n%s\ntxcloud:\n%s\nali:\n%s", baiduExport, tencentExport, txCloudExport, aliExport), nil
}
