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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/collector"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
)

var busyClusters = map[string]int{
	"zybapp-search":              3,
	"matrix-kdcommon":            2,
	"matrix-behavior":            2,
	"matrix-abtest":              2,
	"ocr-search-dprotect":        2,
	"user-unique-identification": 2,
	"zybapp-ugc":                 1,
	"super-strategy-common":      1,
	"matrix-kousuan":             1,
	"zybapp-plat":                1,
	"matrix-vip":                 1,
	"zybapp-sanxia":              1,
}

type ClusterInfoInput struct {
	Budgets   string `form:"budgets"`
	Idcs      string `form:"idcs"`
	IsVirtual int    `form:"isVirtual"`
	Ip        string `form:"ip"`
}

var _ servicer.Servicer = new(ClusterInfoInput)

func (input *ClusterInfoInput) CheckParams(ctx *gin.Context) error {
	if (len(input.Budgets) == 0 || len(input.Idcs) == 0) && len(input.Ip) <= 0 {
		return errors.New("invalid budget or idc")
	}
	return nil
}

func (input *ClusterInfoInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	budgets := make([]string, 0)
	idcs := make([]string, 0)
	machineList := make([]*tbl_machine.Machine, 0)
	if len(input.Ip) <= 0 {
		budgets = strings.Split(input.Budgets, ",")
		if len(budgets) <= 0 {
			err := errors.New("param error")
			return nil, err
		}

		idcs = strings.Split(input.Idcs, ",")
		if len(idcs) <= 0 {
			err := errors.New("param error")
			return nil, err
		}
	} else {
		minfo, err := tbl_machine.GetMachineInfo(input.Ip)
		if err != nil {
			return nil, err
		}
		budgets = append(budgets, minfo.Budget)
		idcs = append(idcs, minfo.IDC)
		machineList = append(machineList, minfo)
	}

	proxyClusterMap := make(map[uint]*tbl_cluster.Cluster)
	serverClusterMap := make(map[uint]*tbl_cluster.Cluster)
	psClusterList, _ := tbl_cluster.GetClusterServerProxyList()
	for _, ps := range psClusterList {
		switch ps.ServiceId {
		case def.SERVICE_ID_BITALOS, def.SERVICE_ID_MATRIX:
			serverClusterMap[ps.Id] = ps
		case def.SERVICE_ID_PROXY:
			proxyClusterMap[ps.Id] = ps
		}
	}

	clusterIdList := make(map[uint]uint, 0)
	proxyClusterIds := make(map[uint]uint, 0)
	machineStatList := make([]*MachineClusterInfo, 0)
	var err error
	for _, b := range budgets {
		for _, idc := range idcs {
			if len(input.Ip) <= 0 {
				machineList, err = tbl_machine.GetMachinesByBudgetIdc(b, idc, input.IsVirtual)
				if err != nil {
					continue
				}
			}
			for _, m := range machineList {
				var bi int
				machineRow := new(MachineClusterInfo)
				machineRow.Machine = m
				ip := m.IP
				bitalosCluster, _ := tbl_node.GetMachineOnlineNodeCount(m.ID, []uint{def.SERVICE_ID_BITALOS, def.SERVICE_ID_MATRIX})
				for _, c := range bitalosCluster {
					if c.ClusterId == 0 {
						continue
					}
					b := MachineClusterStat{}
					if clusterInfo, ok := serverClusterMap[c.ClusterId]; ok {
						b.ClusterId = c.ClusterId
						b.ClusterName = clusterInfo.Name
						b.MachineNode = c.Total
						b.MasterNum = collector.GetServerMasterNum(ip, clusterInfo.Name)
						if b.MasterNum > 0 {
							machineRow.MasterCount += int(b.MasterNum)
						}

						base := getBusyBase(clusterInfo.Name)
						if base > 0 {
							if b.MasterNum == 1 {
								bi += base * 2
							}
						}
					}

					machineRow.Server = append(machineRow.Server, b)
					clusterIdList[c.ClusterId] = 1
				}

				proxyCluster, _ := tbl_node.GetMachineOnlineNodeCount(m.ID, []uint{def.SERVICE_ID_PROXY})
				for _, c := range proxyCluster {
					if c.ClusterId == 0 {
						continue
					}
					b := MachineClusterStat{}
					if clusterInfo, ok := proxyClusterMap[c.ClusterId]; ok {
						b.ClusterId = c.ClusterId
						b.ClusterName = clusterInfo.Name
						b.MachineNode = c.Total
					}

					machineRow.Proxy = append(machineRow.Proxy, b)
					proxyClusterIds[c.ClusterId] = 1
				}
				nodeCluster, _ := tbl_node.GetMachineAllNodes(m.ID, []uint{def.SERVICE_ID_BITALOS, def.SERVICE_ID_PROXY}, def.NODE_NOT_WITNESS)
				for _, n := range nodeCluster {
					if n.ServiceId == def.SERVICE_ID_PROXY {
						d := &MachineClusterCpu{
							Port: n.ServicePort,
						}
						if clusterInfo, ok := proxyClusterMap[n.ClusterId]; ok {
							d.ClusterName = clusterInfo.Name
						}
						sinResource, err := tbl_resource_pool.GetSingleResourceCpu(n.ClusterId, m.IDC)
						if err != nil {
							log.Errorf("GetSingleResource fail err=%v", err)
						}
						if len(sinResource) > 0 {
							d.CpuNums = sinResource[0].CgroupLimit
							d.CpuSetType = sinResource[0].CpuSetType
							d.CpuSetTypeStr = stringCpuSetType(d.CpuSetType)
							d.CpuSet = n.CosFileVersion
							machineRow.ProxyCpuNum += int(d.CpuNums)
						}
						machineRow.ProxyCpu = append(machineRow.ProxyCpu, d)
					}
					if n.ServiceId == def.SERVICE_ID_BITALOS {
						d := &MachineClusterCpu{
							Port: n.ServicePort,
						}
						if clusterInfo, ok := serverClusterMap[n.ClusterId]; ok {
							d.ClusterName = clusterInfo.Name
						}
						sinResource, err := tbl_resource_pool.GetSingleResourceCpu(n.ClusterId, m.IDC)
						if err != nil {
							log.Errorf("GetSingleResource fail err=%v", err)
						}
						if len(sinResource) > 0 {
							d.CpuNums = sinResource[0].CgroupLimit
							d.CpuSetType = sinResource[0].CpuSetType
							d.CpuSetTypeStr = stringCpuSetType(d.CpuSetType)
							machineRow.ServerCpuNum += int(d.CpuNums)
							d.CpuSet = n.CosFileVersion
						}
						machineRow.ServerCpu = append(machineRow.ServerCpu, d)
					}
				}

				machineRow.TotalCpu = machineRow.ProxyCpuNum + machineRow.ServerCpuNum
				machineRow.BusyIndex = bi
				machineStatList = append(machineStatList, machineRow)
			}
		}
	}

	clusterTotalCounter := make(map[uint]uint, 0)
	for clusterId := range clusterIdList {
		s, err := tbl_node.StatCluster(clusterId, nil, false)
		if err != nil {
			log.Warnf("stat cluster fail. clusterId=%d, err=%v", clusterId, err)
			continue
		}
		ns, ok := s.(tbl_node.NodeStat)
		if !ok {
			continue
		}
		clusterTotalCounter[clusterId] = uint(ns.Total)
	}
	proxyTotalCounter := make(map[uint]uint, 0)
	for clusterId := range proxyClusterIds {
		proxyTotalCounter[clusterId] = 0
		s, err := tbl_node.StatCluster(clusterId, nil, false)
		if err != nil {
			log.Warnf("stat cluster fail. clusterId=%d, err=%v", clusterId, err)
			continue
		}
		ns, ok := s.(tbl_node.NodeStat)
		if !ok {
			continue
		}
		proxyTotalCounter[clusterId] = uint(ns.Total)
	}

	proxyClusterCounter := make(map[string]map[string]map[uint]uint, 0)

	budgetIdcClusterCounter := make(map[string]map[string]map[uint]uint, 0)
	for _, b := range budgets {
		budgetIdcClusterCounter[b] = make(map[string]map[uint]uint, 0)
		proxyClusterCounter[b] = make(map[string]map[uint]uint, 0)
		for _, idc := range idcs {
			budgetIdcClusterCounter[b][idc] = make(map[uint]uint, 0)
			proxyClusterCounter[b][idc] = make(map[uint]uint, 0)
			machineList, err := tbl_machine.GetMachinesByBudgetIdc(b, idc, input.IsVirtual)
			if err != nil {
				continue
			}
			machineIds := make([]uint, 0)
			for _, m := range machineList {
				machineIds = append(machineIds, m.ID)
			}
			for clusterId := range clusterIdList {
				clusterStat, err := tbl_node.StatCluster(clusterId, machineIds, false)
				if err != nil {
					continue
				}
				total := clusterStat.(tbl_node.NodeStat).Total
				budgetIdcClusterCounter[b][idc][clusterId] = uint(total)
			}

			for id := range proxyClusterIds {
				clusterStat, err := tbl_node.StatCluster(id, machineIds, false)
				if err != nil {
					continue
				}
				total := clusterStat.(tbl_node.NodeStat).Total
				proxyClusterCounter[b][idc][id] = uint(total)
			}
		}
	}

	for _, m := range machineStatList {
		budget := m.Machine.Budget
		idc := m.Machine.IDC
		for i, s := range m.Server {
			m.Server[i].ClusterNode = budgetIdcClusterCounter[budget][idc][s.ClusterId]
			m.Server[i].TotalNode = clusterTotalCounter[s.ClusterId]
		}
		for i, p := range m.Proxy {
			m.Proxy[i].ClusterNode = proxyClusterCounter[budget][idc][p.ClusterId]
			m.Proxy[i].TotalNode = proxyTotalCounter[p.ClusterId]
		}
	}

	sort.Slice(machineStatList, func(i, j int) bool {
		if machineStatList[i].Budget != machineStatList[j].Budget {
			return machineStatList[i].Budget < machineStatList[j].Budget
		} else if machineStatList[i].IDC != machineStatList[j].IDC {
			return machineStatList[i].IDC < machineStatList[j].IDC
		} else if machineStatList[i].BusyIndex != machineStatList[j].BusyIndex {
			return machineStatList[i].BusyIndex < machineStatList[j].BusyIndex
		} else {
			return len(machineStatList[i].Server) <= len(machineStatList[j].Server)
		}
	})

	output := MachineClusterInfoOutput{}
	output.Rows = machineStatList
	return output, nil
}

func stringCpuSetType(t int) string {
	switch t {
	case def.EXCLUSIVE_CPU:
		return "E"
	case def.SHARE_CPU:
		return "S"
	case def.NOT_SET_CPU:
		return "N"
	}
	return ""
}

func getBusyBase(cluster string) int {
	if v, ok := busyClusters[cluster]; ok {
		return v
	}
	return 0
}

type MachineClusterInfoOutput struct {
	Rows []*MachineClusterInfo `json:"rows"`
}

type MachineClusterInfo struct {
	*tbl_machine.Machine `json:"machine"`
	Proxy                []MachineClusterStat `json:"proxy"`
	Server               []MachineClusterStat `json:"server"`
	ProxyCpu             []*MachineClusterCpu `json:"proxyCpu"`
	ServerCpu            []*MachineClusterCpu `json:"serverCpu"`

	ProxyCpuNum  int `json:"proxyCpuNum"`
	ServerCpuNum int `json:"serverCpuNum"`
	TotalCpu     int `json:"totalCpu"`
	BusyIndex    int `json:"busyIndex"`
	MasterCount  int `json:"masterCount"`
}

type MachineClusterStat struct {
	ClusterId   uint   `json:"clusterId"`
	ClusterName string `json:"clustername"`
	MasterNum   uint8  `json:"masterNum"`
	MachineNode uint   `json:"machineNode"`
	ClusterNode uint   `json:"clusterNode"`
	TotalNode   uint   `json:"totalNode"`
}

type MachineClusterCpu struct {
	ClusterName   string `json:"clusterName"`
	Port          uint   `json:"port"`
	CpuSet        string `json:"cpuSet"`
	CpuNums       int64  `json:"cpuNums"`
	CpuSetType    int    `json:"cpuSetType"`
	CpuSetTypeStr string `json:"cpuSetTypeStr"`
}
