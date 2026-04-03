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
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
)

type ClusterDeployInfoInput struct {
	ClusterId uint `form:"clusterId"` // server cluster id
}

var _ servicer.Servicer = new(ClusterDeployInfoInput)

func (input *ClusterDeployInfoInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid cluster id")
	}
	return nil
}

func (input *ClusterDeployInfoInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	clusterId := input.ClusterId

	cluster, err := tbl_cluster.GetInfo(clusterId)
	if err != nil {
		return nil, err
	}

	proxyInfo, _ := tbl_cluster.GetInfoByStoredId(cluster.StoredId, def.SERVICE_ID_PROXY)

	totalNode, _ := tbl_node.StatCluster(cluster.Id, nil, false)
	totalServer := totalNode.(tbl_node.NodeStat).Total

	totalProxyNode, _ := tbl_node.StatCluster(proxyInfo.Id, nil, false)
	totalProxy := totalProxyNode.(tbl_node.NodeStat).Total

	proxyServerStat := make([]IdcProxyServer, 0)
	for _, b := range def.AllMachineBudgets() {
		machineList, err := tbl_machine.GetMachinesByBudget(b)
		if err != nil {
			continue
		}

		for _, idc := range def.AllMachineIdc() {
			machineIds := make([]uint, 0)
			for _, machine := range machineList {
				if machine.IDC == idc {
					machineIds = append(machineIds, machine.ID)
				}
			}

			proxyServerRow := IdcProxyServer{}
			proxyServerRow.MachineBudget = b
			proxyServerRow.MachineIdc = idc

			currentServerNodeNum := uint(0)
			if len(machineIds) > 0 {
				nodeStats, err := tbl_node.StatCluster(cluster.Id, machineIds, false)
				if err != nil {
					continue
				}
				currentServerNodeNum = uint(nodeStats.(tbl_node.NodeStat).Total)
			}

			proxyServerRow.Server.CurrentNode = currentServerNodeNum
			proxyServerRow.Server.NodeTotal = uint(totalServer)

			if proxyInfo == nil {
				continue
			}

			currentProxyNum := uint(0)
			if len(machineIds) > 0 {
				nodeStats, err := tbl_node.StatCluster(proxyInfo.Id, machineIds, false)
				if err != nil {
					continue
				}
				currentProxyNum = uint(nodeStats.(tbl_node.NodeStat).Total)
			}

			proxyServerRow.Proxy.CurrentNode = currentProxyNum
			proxyServerRow.Proxy.NodeTotal = uint(totalProxy)

			proxyServerStat = append(proxyServerStat, proxyServerRow)
		}
	}

	output := ClusterDeployInfoOutput{}
	output.ClusterInfo = cluster
	output.Proxy = proxyInfo
	output.IdcList = proxyServerStat
	return output, nil
}

func allMachineBudgets() []string {
	return def.AllMachineBudgets()
}

type ClusterDeployInfoOutput struct {
	ClusterInfo *tbl_cluster.Cluster `json:"cluster"`
	Proxy       *tbl_cluster.Cluster `json:"proxy"`
	IdcList     []IdcProxyServer     `json:"idcList"`
}

type IdcProxyServer struct {
	MachineBudget string          `json:"machineBudget"`
	MachineIdc    string          `json:"machineIdc"`
	Proxy         ProxyServerStat `json:"proxy"`
	Server        ProxyServerStat `json:"server"`
}

type ProxyServerStat struct {
	CurrentNode uint `json:"currentNode"`
	NodeTotal   uint `json:"nodeTotal"`
}
