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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"sort"
)

type ClusterDeployServerDetailInput struct {
	ClusterId uint `form:"clusterId"`
}

var _ servicer.Servicer = new(ClusterDeployServerDetailInput)

func (input *ClusterDeployServerDetailInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid cluster id")
	}
	return nil
}

func (input *ClusterDeployServerDetailInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	clusterId := input.ClusterId

	if clusterId == 145 {
		clusterId = 146
	}
	cluster, err := tbl_cluster.GetInfo(clusterId)
	if err != nil {
		return nil, err
	}

	// totalNode, _ := tbl_node.StatCluster(clusterId, nil, false)
	// totalServer := totalNode.(tbl_node.NodeStat).Total

	serverStat := make([]*BudgetMachineList, 0)
	for _, idc := range def.AllMachineIdc() {
		for _, b := range def.AllMachineBudgets() {
			machineList, err := tbl_machine.GetMachinesByBudgetIdc(b, idc, 2)
			if err != nil {
				log.Warnf("GetMachinesByBudgetIdc err(%s), budget:%s, idc:%s", err, b, idc)
				continue
			}

			machineIds := make([]uint, 0)
			for _, machine := range machineList {
				if machine.IDC == idc {
					machineIds = append(machineIds, machine.ID)
				}
			}

			// node := ClusterBudgetIdcStat{}
			// node.MachineIdc = idc
			// node.MachineBudget = b
			// node.NodeIdcTotal = uint(totalServer)

			if len(machineIds) <= 0 {
				continue
			}

			if len(machineIds) > 0 {
				machineStats, err := tbl_node.ListNodeForStat(cluster.Id, machineIds, false)
				if err != nil {
					continue
				}
				nodeStat := machineStats.([]*tbl_node.MachineNodeStat)

				sStat := new(BudgetMachineList)
				sStat.Budget = b
				sStat.Idc = idc
				sStat.MachineNode = make([]*MachineNodeLevelStat, 0)
				for _, ns := range nodeStat {
					levelStat := new(MachineNodeLevelStat)
					levelStat.MachineId = ns.MachineId
					machine := findMachineInfo(ns.MachineId, machineList)
					if machine != nil {
						levelStat.MachineIp = machine.IP
					}
					levelStat.CurrentNode = ns.Total
					sStat.MachineNode = append(sStat.MachineNode, levelStat)
				}
				serverStat = append(serverStat, sStat)
			}
		}
	}
	calcStatByMachine(serverStat)

	sort.Slice(serverStat, func(i, j int) bool {
		if serverStat[i].Idc != serverStat[j].Idc {
			return serverStat[i].Idc < serverStat[j].Idc
		}
		return serverStat[i].Budget <= serverStat[j].Budget
	})

	output := ClusterDeployServerDetailOutput{}
	output.ClusterInfo = cluster
	output.ServerStat = serverStat
	return output, nil
}

func findMachineInfo(id uint, machineList []*tbl_machine.Machine) *tbl_machine.Machine {
	for _, m := range machineList {
		if m.ID == id {
			return m
		}
	}
	return nil
}

func calcStatByMachine(serverStat []*BudgetMachineList) {
	// init
	idcList := def.AllMachineIdc()
	nodeTotal := uint(0)
	idcCounter := make(map[string]uint, len(idcList))
	nodeCounter := make(map[string](map[string]uint), len(idcList))

	for _, idc := range idcList {
		budgetList := def.AllMachineBudgets()
		nodeCounter[idc] = make(map[string]uint, len(budgetList))
		for _, budget := range budgetList {
			nodeCounter[idc][budget] = 0
		}
	}

	for _, stat := range serverStat {
		budget := stat.Budget
		idc := stat.Idc
		for _, machineNode := range stat.MachineNode {
			if machineNode == nil {
				continue
			}
			nodeTotal += machineNode.CurrentNode
			nodeCounter[idc][budget] += machineNode.CurrentNode
			idcCounter[idc] += machineNode.CurrentNode
		}
	}

	for _, stat := range serverStat {
		budget := stat.Budget
		idc := stat.Idc
		for _, machineNode := range stat.MachineNode {
			if machineNode == nil {
				continue
			}
			machineNode.BudgetNode = nodeCounter[idc][budget]
			machineNode.IdcNode = idcCounter[idc]
			machineNode.NodeTotal = nodeTotal
		}
	}
	fmt.Println(nodeTotal, idcCounter, nodeCounter)
}

type ClusterDeployServerDetailOutput struct {
	ClusterInfo *tbl_cluster.Cluster `json:"cluster"`
	ServerStat  []*BudgetMachineList `json:"serverStat"`
}

type BudgetMachineList struct {
	Idc         string                  `json:"idc"`
	Budget      string                  `json:"budget"`
	MachineNode []*MachineNodeLevelStat `json:"machines"`
}

type MachineNodeLevelStat struct {
	MachineId   uint   `json:"machineId"`
	MachineIp   string `json:"machineIp"`
	CurrentNode uint   `json:"currentNode"`
	BudgetNode  uint   `json:"budgetNode"`
	IdcNode     uint   `json:"idcNode"`
	NodeTotal   uint   `json:"nodeTotal"`
}
