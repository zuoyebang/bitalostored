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
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_machine"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"sort"
)

type MachineInfosInput struct {
	RegionId  uint `form:"regionId"`
	ClusterId uint `form:"clusterId"`
}

var _ servicer.Servicer = new(MachineInfosInput)

func (input *MachineInfosInput) CheckParams(ctx *gin.Context) error {
	if input.RegionId <= 0 {
		return errors.New("invalid regionId")
	}
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	return nil
}

func (input *MachineInfosInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	clusterInfo, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil {
		log.Warn("get cluster info failed.err:", err)
		return nil, err
	}
	regionInfo, err := tbl_region.GetInfo(clusterInfo.RegionId)
	if err != nil || regionInfo == nil {
		log.Warnf("get region info failed.err:%+v", err)
	}
	regionId := clusterInfo.RegionId
	if regionInfo.NewId > 0 {
		regionId = regionInfo.NewId
	}
	machineList := mdl_machine.GetMachinesByRegion(regionId, []string{def.MACHINE_STATUS_ONLINE})
	if machineList == nil {
		log.Warn("failed to get region machines.")
		return errors.New("failed to get region machines"), nil
	}
	var machineInfoList MachineInfosOutput

	nodeList, err := tbl_node.GetListByCluster(input.ClusterId)
	if err != nil {
		log.Warn("get node list failed.err:", err)
		return nil, err
	}
	mNode := make(map[uint]int, 0)
	for _, node := range nodeList {
		if node.Status != def.NODE_STATUS_OFFLINE && !node.IsWitness {
			mNode[node.MachineId]++
		}
	}
	serviceInfo, _ := tbl_service.GetInfoByName(def.SERVICE_MATRIX)
	idcMachines := make(map[string][]*MachineInfo, 0)
	log.Infof("node distribute:%+v", mNode)
	for _, m := range machineList {
		mInfo := MachineInfo{}
		mInfo.Machine = m
		mInfo.ClusterNodeSum = mNode[m.ID]
		idcMachines[m.IDC] = append(idcMachines[m.IDC], &mInfo)
		matrixNodes, _ := tbl_node.GetMachineOnlineNodes(m.ID, serviceInfo.ID, false)
		mInfo.TotalNodeSum = len(matrixNodes)
	}
	for _, list := range idcMachines {
		mList := list
		sort.Sort(MachineSorter(mList))
		machineInfoList.MachineInfos = append(machineInfoList.MachineInfos, mList...)
	}
	return machineInfoList, nil

}

type MachineInfosOutput struct {
	MachineInfos []*MachineInfo `json:"machineInfos"`
}

type MachineInfo struct {
	*tbl_machine.Machine
	BalanceIndex   int  `json:"balanceIndex"`
	ClusterNodeSum int  `json:"clusterNodeSum"`
	TotalNodeSum   int  `json:"totalNodeSum"`
	AgentStatus    bool `json:"agentStatus"`
}

type MachineSorter []*MachineInfo

func (ms MachineSorter) Len() int           { return len(ms) }
func (ms MachineSorter) Less(i, j int) bool { return ms[i].ClusterNodeSum < ms[j].ClusterNodeSum }
func (ms MachineSorter) Swap(i, j int)      { ms[i], ms[j] = ms[j], ms[i] }
