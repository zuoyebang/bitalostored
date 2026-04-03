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
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_config"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type ClusterListInput struct {
	ServiceId   uint   `form:"serviceId"`
	ClusterName string `form:"clusterName"`
	Department  string `form:"department"`
	Page        int    `form:"page"`
	Num         int    `form:"num"`
}

var _ servicer.Servicer = new(ClusterListInput)

func (input *ClusterListInput) CheckParams(ctx *gin.Context) error {
	if input.ServiceId == 0 {
		return errors.New("invalid serviceId")
	}
	if input.Page == 0 {
		input.Page = 1
	}
	if input.Num == 0 {
		input.Num = 20
	}
	return nil
}

func (input *ClusterListInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	var clusterInfos []*tbl_cluster.Cluster
	var err error
	if len(input.ClusterName) > 0 {
		clusterInfos, err = tbl_cluster.GetListByNs(input.ClusterName, input.ServiceId)
		if err != nil {
			log.Warnf("GetListByNs failed err:%v", err)
			return nil, err
		}
	} else {
		clusterInfos, err = tbl_cluster.GetListByDepartment(input.ServiceId, input.Department, input.Page, input.Num)
		if err != nil {
			log.Warnf("get department cluster failed.err:%+v", err)
			return nil, err
		}
	}
	packList, err := tbl_config.ConfigPackList(input.ServiceId)
	if err != nil {
		log.Warn("get config info failed.err:", err)
		return nil, err
	}
	configPackMap := make(map[string]uint, 0)
	for _, pack := range packList {
		configPackMap[pack.ConfigPackName] = pack.ConfigPackId
	}

	if input.ServiceId == def.SERVICE_ID_FE || input.ServiceId == def.SERVICE_ID_DASHBOARD {
		var output StoredListOutput
		for name, id := range configPackMap {
			output.StoredList = append(output.StoredList, StoredInfo{
				ClusterName:  name,
				ConfigPackId: id,
			})
		}
		if len(clusterInfos) > 0 {
			var clusterIds, machineIds []uint
			for _, cluster := range clusterInfos {
				clusterIds = append(clusterIds, cluster.Id)
			}
			nodes, nodeErr := tbl_node.GetOnlineListByClusterIds(clusterIds)
			if nodeErr != nil {
				log.Warnf("get node info failed.err:%v", nodeErr)
				return nil, nodeErr
			}
			nodeInfos := make(map[uint]*tbl_node.Node, len(nodes))
			for _, node := range nodes {
				nodeInfos[node.ClusterId] = node
				machineIds = append(machineIds, node.MachineId)
			}
			machineInfos, machineErr := tbl_machine.GetList(machineIds)
			if machineErr != nil {
				log.Warnf("get machine info failed.err:%v", machineErr)
				return nil, machineErr
			}
			machineMap := make(map[uint]*tbl_machine.Machine, len(machineInfos))
			for _, m := range machineInfos {
				machineMap[m.ID] = m
			}
			for _, cluster := range clusterInfos {
				node := &tbl_node.Node{}
				if _, ok := nodeInfos[cluster.Id]; ok {
					node = nodeInfos[cluster.Id]
				}
				var budget, ip string
				if machineInfo, ok := machineMap[node.MachineId]; ok {
					budget = machineInfo.Budget
					ip = machineInfo.IP
				} else {
					continue
				}
				output.StoredList = append(output.StoredList, StoredInfo{
					Node:         node,
					ClusterName:  cluster.Name,
					Department:   cluster.Department,
					ConfigPackId: cluster.ConfigPackId,
					RegionName:   "",
					StoredId:     cluster.StoredId,
					Budget:       budget,
					IP:           ip,
					JumpAddress:  "",
				})
			}
		}
		output.Count, _ = tbl_cluster.GetCountByDepartment(input.ServiceId, input.Department)
		return output, nil
	}
	var output ClusterListOutput
	for name, id := range configPackMap {
		output.Rows = append(output.Rows, ClusterInfo{
			Cluster: &tbl_cluster.Cluster{Name: name, ConfigPackId: id},
		})
	}
	for _, info := range clusterInfos {
		regionInfo, _ := tbl_region.GetInfo(info.RegionId)
		regionName := regionInfo.Name
		if regionInfo.NewId > 0 {
			newRegionInfo, _ := tbl_region.GetInfo(regionInfo.NewId)
			regionName = regionName + "(" + newRegionInfo.Name + ")"
		}
		output.Rows = append(output.Rows, ClusterInfo{
			Cluster:       info,
			RegionName:    regionName,
			DashboardAddr: fmt.Sprintf(config.GetConf().Domains.DashboardDomain+"/#/%s", info.Name),
		})
	}
	output.Count, _ = tbl_cluster.GetCountByDepartment(input.ServiceId, input.Department)
	return output, nil
}

type ClusterListOutput struct {
	Count int64         `json:"count"`
	Rows  []ClusterInfo `json:"rows"`
}

type ClusterInfo struct {
	*tbl_cluster.Cluster
	RegionName    string `json:"regionName"`
	DashboardAddr string `json:"dashboardAddr"`
}

type StoredListOutput struct {
	StoredList []StoredInfo `json:"rows"`
	Count      int64        `json:"count"`
	ServiceId  uint         `json:"serviceId"`
}

type StoredInfo struct {
	ClusterName  string `json:"clusterName"`
	ConfigPackId uint   `json:"configPackId"`
	StoredId     uint   `json:"storedId"`
	RegionName   string `json:"regionName"`
	Department   string `json:"department"`
	IP           string `json:"ip"`
	Budget       string `json:"budget"`
	*tbl_node.Node
	JumpAddress string `json:"jumpAddress"`
}
