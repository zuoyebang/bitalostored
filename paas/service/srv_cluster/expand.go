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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cosfile"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_group"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_regionmachine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_group"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_port"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"sort"
)

type ExpandInput struct {
	ClusterId    uint   `json:"clusterId"`
	TargetIPList []uint `json:"targetMachine"`
	CosFileId    uint   `json:"packageId"`
	Role         string `json:"role"`
	Num          uint   `json:"nodeNum"`
	IDC          string `json:"idc"`
}

var _ servicer.Servicer = new(ExpandInput)

func (input *ExpandInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId == 0 {
		return errors.New("invalid clusterId")
	}
	if input.Num <= 0 || input.Num > 3 {
		return errors.New("invalid nodeNum. 1 <= nodeNum <= 3")
	}
	// if len(input.TargetIPList) == 0 {
	// 	input.TargetIPList = []uint{1}
	// 	// return errors.New("invalid targetMachine")
	// }
	if input.Role == "" {
		return errors.New("invalid role")
	}
	if input.IDC == "" {
		return errors.New("invalid idc")
	}
	if input.CosFileId <= 0 {
		return errors.New("invalid packageId")
	}
	return nil
}

func (input *ExpandInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	var err error
	clusterInfo, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil {
		log.Warn("get basic info failed.err:", err)
		return nil, err
	}
	regionInfo, err := tbl_region.GetInfo(clusterInfo.RegionId)
	if err != nil {
		log.Warn("get region info failed.err:", err)
		return nil, err
	}
	cosFileInfo, err := tbl_cosfile.GetCosFile(input.CosFileId)
	if err != nil {
		return nil, err
	}
	if cosFileInfo.ID <= 0 {
		return nil, errors.New("can't find cos file")
	}
	regionName := regionInfo.Name
	regionId := clusterInfo.RegionId
	if regionInfo.NewId > 0 {
		regionId = regionInfo.NewId
	}
	serviceInfo, err := tbl_service.GetInfo(clusterInfo.ServiceId)
	if err != nil {
		log.Warn("get service info failed.err:", err)
		return nil, err
	}
	if serviceInfo.Name != def.SERVICE_MATRIX && serviceInfo.Name != def.SERVICE_BITALOS {
		return nil, errors.New("support matrix expansion only.")
	}
	machineInfos, err := tbl_machine.GetOnlineList(input.TargetIPList, input.IDC)
	if err != nil {
		log.Warn("get machine info failed.err:", err)
		return nil, err
	}
	if len(machineInfos) == 0 {
		machineIdList, err := tbl_regionmachine.GetMachinesByRegion(regionId)
		if err != nil {
			log.Warn("no candidate machines.err:", err)
			return nil, err
		}
		machineList, err := tbl_machine.GetOnlineList(machineIdList, input.IDC)
		if err != nil {
			log.Warnf("failed to get region machines.err:%+v", err)
			return nil, err
		}
		machineInfos = machineList
		if len(machineInfos) == 0 {
			return nil, errors.New("no machine to deploy instance")
		}
	}
	for i, m := range machineInfos {
		nodes, err := tbl_node.GetClusterMachineCountByRole(m.ID, clusterInfo.ServiceId, clusterInfo.Id, []string{def.NODE_STATUS_ONLINE}, input.Role == def.NODE_ROLE_WITNESS)
		if err != nil {
			log.Warn("get online nodes for machine ", m.IP, " failed.err:", err)
			machineInfos[i].NodeSum = 0
			continue
		}
		machineInfos[i].NodeSum = uint(nodes)
	}
	dashboardName, err := mdl_dashboard.GetDashboardName(clusterInfo.StoredId)
	if err != nil {
		log.Warn("get dashboard name failed.err:", err)
		return nil, err
	}
	taskType := def.TASK_TYPE_PREPAREADD
	groupMap, err := mdl_group.GetGroupOnlinesNodes(clusterInfo.Id)
	if err != nil {
		log.Warn("get online group failed.err:", err)
		return nil, err
	}
	// var allmachineList []*tbl_machine.Machine
	// if len(machineInfos) >= len(groupMap)*int(input.Num) {
	// 	allmachineList = machineInfos[0 : len(groupMap)*int(input.Num)]
	// } else {
	// 	for len(allmachineList) < len(groupMap)*int(input.Num) {
	// 		allmachineList = append(allmachineList, machineInfos...)
	// 	}
	// 	allmachineList = allmachineList[0 : len(groupMap)*int(input.Num)]
	// }
	witnessCount := make(map[uint]int, 0)
	if input.Role == def.NODE_ROLE_WITNESS {
		for gId, nodeList := range groupMap {
			count := 0
			for _, n := range nodeList {
				if n.IsWitness {
					count++
				}
			}
			witnessCount[gId] = count
		}
	}
	log.Info("group machines:", groupMap, " witness count:", witnessCount)
	assignGroupMachines := make(map[uint][]uint, 0)
	// scanned := make([]bool, len(allmachineList))
	for gId, nodeList := range groupMap {
		sort.Sort(tbl_machine.MachinesSorter(machineInfos))
		log.Info("candidate machines:", machineInfos)
		for j := 0; j < int(input.Num); j++ {
			if witnessCount[gId]+j >= int(input.Num) {
				continue
			}
			index := -1
			for i, machine := range machineInfos {
				if !isMachineInList(machine.ID, nodeList) && !isMachineHasAssigned(machine.ID, assignGroupMachines[gId]) {
					index = i
					break
				}
			}
			if index < 0 {
				for i, machine := range machineInfos {
					if !isMachineHasAssigned(machine.ID, assignGroupMachines[gId]) {
						index = i
						break
					}
				}
			}
			if index < 0 {
				index = 0
				sort.Sort(tbl_machine.MachinesSorter(machineInfos))
				log.Info("need to sort machines:", machineInfos)
			}
			machineInfo := machineInfos[index]
			initRaft, groupInfo, err := mdl_group.GetGroupInfo(input.ClusterId, gId, clusterInfo.ServiceId)
			if err != nil {
				log.Warn("get group info failed.err:", err)
				return nil, err
			}
			nodeInfo := &tbl_node.Node{
				ClusterId:      input.ClusterId,
				GroupId:        gId,
				CosFileId:      input.CosFileId,
				CosFileVersion: cosFileInfo.Version,
				RegionId:       regionId,
				MachineId:      machineInfo.ID,
				ServiceId:      clusterInfo.ServiceId,
				IsWitness:      input.Role == def.NODE_ROLE_WITNESS,
			}
			pod, err := tbl_node.Create(nodeInfo, groupInfo.MaxNodeId)
			if err != nil {
				log.Warn("create node failed.err:", err)
				return nil, err
			}
			e := tbl_group.Update(clusterInfo.Id, gId, tbl_group.Group{
				MaxNodeId: pod.NodeId,
			})
			if e != nil {
				log.Warn("update maxNodeId failed.err:", err)
				return nil, err
			}
			assignGroupMachines[gId] = append(assignGroupMachines[gId], machineInfo.ID)
			machineInfos[index].NodeSum = machineInfos[index].NodeSum + 1
			task := &tbl_task.Task{
				Type:      taskType,
				Status:    def.TASK_NEW,
				RegionId:  regionId,
				MachineId: machineInfo.ID,
				ServiceId: clusterInfo.ServiceId,
				ClusterId: input.ClusterId,
				GroupId:   gId,
				NodeId:    pod.NodeId,
				CosFileId: input.CosFileId,
				TaskExt: tbl_task.TaskExtra{
					Ip:               machineInfo.IP,
					RegionName:       regionName,
					ServiceName:      serviceInfo.Name,
					ServicePortRange: mdl_port.NarrowDownPortRange(serviceInfo.PortRanges, machineInfo.ID),
					ClusterPortRange: mdl_port.NarrowDownPortRange(serviceInfo.ClusterPortRanges, machineInfo.ID),
					ClusterName:      clusterInfo.Name,
					DashboardName:    dashboardName,
					CloudType:        machineInfo.IDC,
					NodeIndex:        int(pod.NodeId),
					NodeList:         initRaft,
					NodeIdList:       groupInfo.InitNodeId,
					Operation:        def.OPERATION_SUPERVISOR_START,
					IsWitness:        input.Role == def.NODE_ROLE_WITNESS,
					IsObserver:       input.Role == def.NODE_ROLE_OBSERVER,
					DeraftToken:      math2.GetMd5(clusterInfo.Name),
					UpdateConfig:     true,
				},
			}
			err = tbl_task.CreateTask(task)
			if err != nil {
				log.Warn("update task info failed.err:", err)
			}
			log.Infof("add task:%+v", task)
		}
	}
	return nil, nil
}

func isMachineHasAssigned(id uint, had []uint) bool {
	for _, mId := range had {
		if id == mId {
			return true
		}
	}
	return false
}

func isMachineInList(id uint, list []*tbl_node.Node) bool {
	for _, n := range list {
		if n.MachineId == id {
			return true
		}
	}
	return false
}
