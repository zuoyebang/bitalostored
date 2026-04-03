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
	"strings"
)

type DeployOverviewInput struct {
	Budgets string `form:"budgets"`
	Idcs    string `form:"idcs"`
}

var _ servicer.Servicer = new(DeployOverviewInput)

func (input *DeployOverviewInput) CheckParams(ctx *gin.Context) error {
	fmt.Println(input)
	if len(input.Budgets) == 0 || len(input.Idcs) == 0 {
		return errors.New("invalid budget or idc")
	}
	return nil
}

func (input *DeployOverviewInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	budgets := strings.Split(input.Budgets, ",")
	if len(budgets) <= 0 {
		err := errors.New("param error")
		return nil, err
	}

	idcs := strings.Split(input.Idcs, ",")
	if len(idcs) <= 0 {
		err := errors.New("param error")
		return nil, err
	}
	clusterList, err := tbl_cluster.GetClusterServerList()
	if err != nil {
		return nil, err
	}
	clusterMap := make(map[uint]*tbl_cluster.Cluster, 0)
	for _, c := range clusterList {
		clusterMap[c.Id] = c
	}

	clusterStat := make([]*ClusterBudgetIdcStat, 0)
	allClusterOverview := make([]ClusterGroupIdcStat, 0)
	for _, b := range budgets {
		if !checkBudgetValid(b) {
			continue
		}

		machineList, err := tbl_machine.GetMachinesByBudget(b)
		if err != nil {
			continue
		}

		for _, idc := range idcs {
			if !checkIdcValid(idc) {
				continue
			}
			//fmt.Println(idc, b)

			cgStat := ClusterGroupIdcStat{MachineBudget: b, MachineIdc: idc}

			machineIds := make([]uint, 0)
			for _, machine := range machineList {
				if machine.IDC == idc {
					machineIds = append(machineIds, machine.ID)
				}
			}

			allMachineIds := make([]uint, 0)
			allMachineList, err := tbl_machine.GetMachinesByIdc(idc)
			if err != nil {
				continue
			}
			for _, machine := range allMachineList {
				allMachineIds = append(allMachineIds, machine.ID)
			}

			for _, cluster := range clusterList {
				stat := new(ClusterBudgetIdcStat)
				stat.ClusterId = cluster.Id
				stat.ClusterName = cluster.Name
				stat.ClusterGroup = cluster.ClusterGroup
				stat.MachineBudget = b
				stat.MachineIdc = idc

				if len(machineIds) <= 0 {
					stat.CurrentNode = 0
				} else {
					nodeStats, err := tbl_node.StatCluster(cluster.Id, machineIds, false)
					if nodeStats == nil {
						log.Warnf("get node stats clusterId:%d machineIds:%v err:%v", cluster.Id, machineIds, err)
						stat.CurrentNode = 0
					} else {
						stat.CurrentNode = uint(nodeStats.(tbl_node.NodeStat).Total)
					}
				}
				nodeStat2, err := tbl_node.StatCluster(cluster.Id, allMachineIds, false)
				if nodeStat2 == nil {
					log.Warnf("get node stats clusterId:%d machineIds:%v err:%v", cluster.Id, allMachineIds, err)
					stat.NodeIdcTotal = 0
				} else {
					stat.NodeIdcTotal = uint(nodeStat2.(tbl_node.NodeStat).Total)
				}
				clusterStat = append(clusterStat, stat)

				sumClusterGroup(&cgStat, stat.ClusterGroup, stat.CurrentNode, stat.NodeIdcTotal)
			}
			allClusterOverview = append(allClusterOverview, cgStat)
		}
	}

	sort.Slice(clusterStat, func(i, j int) bool {
		if clusterStat[i].ClusterGroup != clusterStat[j].ClusterGroup {
			return clusterStat[i].ClusterGroup < clusterStat[j].ClusterGroup
		}
		if clusterStat[i].ClusterId != clusterStat[j].ClusterId {
			return clusterStat[i].ClusterId < clusterStat[j].ClusterId
		}
		if clusterStat[i].MachineBudget != clusterStat[j].MachineBudget {
			return clusterStat[i].MachineBudget < clusterStat[j].MachineBudget
		}
		return clusterStat[i].MachineIdc <= clusterStat[j].MachineIdc
	})

	clusterBudgetMap := make(map[string]map[uint]map[string][]CIdcStat, 0)
	for _, s := range clusterStat {
		if _, ok := clusterBudgetMap[s.ClusterGroup]; !ok {
			clusterBudgetMap[s.ClusterGroup] = make(map[uint]map[string][]CIdcStat, 0)
		}
		if _, ok := clusterBudgetMap[s.ClusterGroup][s.ClusterId]; !ok {
			clusterBudgetMap[s.ClusterGroup][s.ClusterId] = make(map[string][]CIdcStat, 0)
		}
		if _, ok := clusterBudgetMap[s.ClusterGroup][s.ClusterId][s.MachineBudget]; !ok {
			clusterBudgetMap[s.ClusterGroup][s.ClusterId][s.MachineBudget] = make([]CIdcStat, 0)
		}
		cs := CIdcStat{}
		cs.MachineIdc = s.MachineIdc
		cs.CurrentNode = s.CurrentNode
		cs.NodeIdcTotal = s.NodeIdcTotal
		clusterBudgetMap[s.ClusterGroup][s.ClusterId][s.MachineBudget] = append(clusterBudgetMap[s.ClusterGroup][s.ClusterId][s.MachineBudget], cs)
	}

	clusterAgg := make([]ClusterBudgetIdcAgg, 0)
	for cg, level1 := range clusterBudgetMap {
		for ci, level2 := range level1 {
			for mb, level3 := range level2 {
				agg := ClusterBudgetIdcAgg{}
				agg.ClusterGroup = cg
				agg.ClusterId = ci
				agg.ClusterName = clusterMap[ci].Name
				agg.MachineBudget = mb
				agg.IdcList = level3
				clusterAgg = append(clusterAgg, agg)
			}
		}
	}

	output := ClusterOverviewOutput{}
	output.ClusterOverview = allClusterOverview
	output.ClusterStat = clusterAgg
	return output, nil
}

func sumClusterGroup(cgStat *ClusterGroupIdcStat, cg string, currentNode uint, nodeTotal uint) {
	switch cg {
	case def.ClusterGroupOcr:
		cgStat.OcrGroupNode += int(currentNode)
		cgStat.OcrGroupTotal += int(nodeTotal)
	case def.ClusterGroupYonghu:
		cgStat.YonghuGroupNode += int(currentNode)
		cgStat.YonghuGroupTotal += int(nodeTotal)
	case def.ClusterGroupLive:
		cgStat.LiveGroupNode += int(currentNode)
		cgStat.LiveGroupTotal += int(nodeTotal)
	case def.ClusterGroupAb:
		cgStat.AbGroupNode += int(currentNode)
		cgStat.AbGroupTotal += int(nodeTotal)
	case def.ClusterGroupBd:
		cgStat.BigdataGroupNode += int(currentNode)
		cgStat.BigdataGroupTotal += int(nodeTotal)
	case def.ClusterGroupTf:
		cgStat.ToufangGroupNode += int(currentNode)
		cgStat.ToufangGroupTotal += int(nodeTotal)
	case def.ClusterGroupLx:
		cgStat.ToufangGroupNode += int(currentNode)
		cgStat.ToufangGroupTotal += int(nodeTotal)
	case def.ClusterGroupPeople:
		cgStat.ToufangGroupNode += int(currentNode)
		cgStat.ToufangGroupTotal += int(nodeTotal)
	}
}

func checkBudgetValid(budget string) bool {
	allBudgets := def.AllMachineBudgets()
	for _, b := range allBudgets {
		if b == budget {
			return true
		}
	}
	return false
}
func checkIdcValid(idc string) bool {
	allIdcs := []string{def.IdcAli, def.IdcBaidu, def.IdcTxcloud, def.IdcTencent, def.IdcTxgz, def.IdcTxsh}
	for _, i := range allIdcs {
		if i == idc {
			return true
		}
	}
	return false
}

type ClusterBudgetIdcAgg struct {
	ClusterName   string     `json:"clusterName"`
	ClusterId     uint       `json:"clusterId"`
	ClusterGroup  string     `json:"clusterGroup"`
	MachineBudget string     `json:"machineBudget"`
	IdcList       []CIdcStat `json:"idcList"`
}

type ClusterBudgetIdcStat struct {
	ClusterName   string `json:"clusterName"`
	ClusterId     uint   `json:"clusterId"`
	ClusterGroup  string `json:"clusterGroup"`
	MachineBudget string `json:"machineBudget"`
	MachineIdc    string `json:"machineIdc"`
	CurrentNode   uint   `json:"currentNode"`
	NodeIdcTotal  uint   `json:"idcNode"`
}

type CIdcStat struct {
	MachineIdc   string `json:"machineIdc"`
	CurrentNode  uint   `json:"currentNode"`
	NodeIdcTotal uint   `json:"idcNode"`
}

type ClusterGroupIdcStat struct {
	MachineBudget     string `json:"machineBudget"`
	MachineIdc        string `json:"machineIdc"`
	OcrGroupNode      int    `json:"ocrGroupNode"`
	OcrGroupTotal     int    `json:"ocrGroupTotal"`
	YonghuGroupNode   int    `json:"yonghuGroupNode"`
	YonghuGroupTotal  int    `json:"yonghuGroupTotal"`
	LiveGroupNode     int    `json:"liveGroupNode"`
	LiveGroupTotal    int    `json:"liveGroupTotal"`
	AbGroupNode       int    `json:"abGroupNode"`
	AbGroupTotal      int    `json:"abGroupTotal"`
	BigdataGroupNode  int    `json:"bigdataGroupNode"`
	BigdataGroupTotal int    `json:"bigdataGroupTotal"`
	ToufangGroupNode  int    `json:"toufangGroupNode"`
	ToufangGroupTotal int    `json:"toufangGroupTotal"`
}

type ClusterOverviewOutput struct {
	ClusterOverview []ClusterGroupIdcStat `json:"clusterOverview"`
	ClusterStat     []ClusterBudgetIdcAgg `json:"clusterStat"`
}

/*
type ClusterListOutput struct {
	Count int           `json:"count"`
	Rows  []ClusterInfo `json:"rows"`
}

*/
