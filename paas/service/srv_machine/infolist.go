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
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_regionmachine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_machine"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"time"
)

type MachineInfoListInput struct {
	Budget string `form:"budget"`
	Ip     string `form:"ip"`
}

var _ servicer.Servicer = new(MachineInfoListInput)

func (input *MachineInfoListInput) CheckParams(ctx *gin.Context) error {
	return nil
}

func (input *MachineInfoListInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	var ms MachineInfoListOutput
	var err error
	var machineList []*tbl_machine.Machine
	if input.Ip != "" {
		machineList, err = tbl_machine.GetMachinesByIpList([]string{input.Ip})
	} else {
		machineList, err = tbl_machine.GetMachinesByBudget(input.Budget)
	}
	if err != nil {
		return nil, err
	}
	clusterList, err := tbl_cluster.GetClusterNames()
	if err != nil {
		return nil, err
	}
	matrixService, _ := tbl_service.GetInfoByName(def.SERVICE_MATRIX)
	proxyService, _ := tbl_service.GetInfoByName(def.SERVICE_STORED_PROXY)
	bitalosService, _ := tbl_service.GetInfoByName(def.SERVICE_BITALOS)
	for _, m := range machineList {
		// matrixCluster, _ := GetServiceNodeDetail(matrixService.ID, m.ID)
		// bitalosCluster, _ := GetServiceNodeDetail(bitalosService.ID, m.ID)
		// proxyCluster, _ := GetServiceNodeDetail(proxyService.ID, m.ID)
		// matrixNodeList, _ := tbl_node.GetMachineServiceCount(m.ID, matrixService.ID, []string{def.NODE_STATUS_ONLINE})
		// matrixNodeList, _ := tbl_node.GetMachineOnlineNodes(m.ID, matrixService.ID, false)
		// bitalosNodeList, _ := tbl_node.GetMachineOnlineNodes(m.ID, bitalosService.ID, false)
		// witnessNodeList, _ := tbl_node.GetMachineOnlineNodes(m.ID, bitalosService.ID, true)
		// proxyCount, _ := tbl_node.GetMachineServiceCount(m.ID, proxyService.ID, []string{def.NODE_STATUS_ONLINE})

		matrixCount, _ := tbl_node.CountMachineNode(m.ID, matrixService.ID, false)
		bitalosCount, _ := tbl_node.CountMachineNode(m.ID, bitalosService.ID, false)
		witnessCount, _ := tbl_node.CountMachineNode(m.ID, bitalosService.ID, true)
		proxyCount, _ := tbl_node.CountMachineNode(m.ID, proxyService.ID, false)
		dashboardCount, _ := tbl_node.CountMachineNode(m.ID, uint(def.SERVICE_ID_DASHBOARD), false)
		feCount, _ := tbl_node.CountMachineNode(m.ID, uint(def.SERVICE_ID_FE), false)
		witnessCNames := mdl_machine.GetClusterNameByMachineId(clusterList, m.ID, bitalosService.ID, true)
		dashboardCNames := mdl_machine.GetClusterNameByMachineId(clusterList, m.ID, uint(def.SERVICE_ID_DASHBOARD), false)

		nodes := createNodeText(bitalosCount, matrixCount, proxyCount, witnessCount, dashboardCount, feCount, witnessCNames, dashboardCNames)
		//nodes := "bitalos:" + strconv.Itoa(len(bitalosCluster)) + "\nmatrix:" + strconv.Itoa(len(matrixCount)) + "\nwitness:" + strconv.Itoa(len(witnessCount)) + "\nproxy:" + strconv.Itoa(proxyCount)
		regions := tbl_regionmachine.GetMachineRegion([]uint{m.ID})
		if m.NeedUpgrade == "" {
			m.NeedUpgrade = def.NEED_UPGRADE_NO
		}
		status := false
		if time.Now().Unix()-m.UpdateTime <= 200 {
			status = true
		}
		ms.MachineInfos = append(ms.MachineInfos, MInfo{
			Machine: m,
			// Matrix:      matrixCluster,
			// Bitalos:     bitalosCluster,
			// Proxy:       proxyCluster,
			Nodes:       nodes,
			Regions:     regions[m.ID],
			AgentStatus: status,
		})
	}
	ms.Count = len(machineList)
	return ms, nil
}

func createNodeText(bitalosCount, matrixCount, proxyCount, witnessCount, dashboardCount, feCount int, witnessCluster, dashboardCluster []string) string {
	var text string
	if bitalosCount > 0 {
		text += fmt.Sprintf("bitalos:%d;", bitalosCount)
	}
	if matrixCount > 0 {
		text += fmt.Sprintf("matrix:%d;", matrixCount)
	}
	if proxyCount > 0 {
		text += fmt.Sprintf("proxy:%d;", proxyCount)
	}
	if witnessCount > 0 {
		text += fmt.Sprintf("witness:%d,%s;", witnessCount, witnessCluster)
	}
	if dashboardCount > 0 {
		text += fmt.Sprintf("dashboard:%d,%s;", dashboardCount, dashboardCluster)
	}
	if feCount > 0 {
		text += fmt.Sprintf("fe:%d;", feCount)
	}
	return text
}

type MachineInfoListOutput struct {
	Count        int     `json:"count"`
	MachineInfos []MInfo `json:"machineInfoList"`
}

type MInfo struct {
	*tbl_machine.Machine
	Matrix      map[string][]uint `json:"matrix"`
	Bitalos     map[string][]uint `json:"bitalos"`
	Proxy       map[string][]uint `json:"proxy"`
	Nodes       string            `json:"nodes"`
	Regions     []uint            `json:"regions"`
	AgentStatus bool              `json:"agentStatus"`
}

func GetServiceNodeDetail(serviceId, machineId uint) (map[string][]uint, error) {
	clusterDetail := make(map[string][]uint)
	clusterMap := make(map[uint]*tbl_cluster.Cluster, 0)
	nodes, err := tbl_node.GetMachineNodes(machineId, serviceId)
	if err != nil {
		log.Warn("get node info failed.err:", err)
		return clusterDetail, err
	}

	for _, n := range nodes {
		_, ok := clusterMap[n.ClusterId]
		if !ok {
			clsuterInfo, err := tbl_cluster.GetInfo(n.ClusterId)
			if err != nil {
				log.Warn("get cluster info failed.err:", err)
				return clusterDetail, err
			}
			clusterMap[n.ClusterId] = clsuterInfo
		}
		clusterDetail[clusterMap[n.ClusterId].Name] = append(clusterDetail[clusterMap[n.ClusterId].Name], n.ServicePort)
	}
	return clusterDetail, nil
}
