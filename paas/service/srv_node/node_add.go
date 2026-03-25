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

package srv_node

import (
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
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"sort"
)

type CreateNodeInput struct {
	ClusterId uint   `json:"clusterId"`
	GroupId   uint   `json:"groupId"`
	ServiceId uint   `json:"serviceId"`
	CosFileId uint   `json:"packageId"`
	MachineId []uint `json:"machineId"`
	RegionId  uint   `json:"regionId"`
	NodeRole  string `json:"nodeRole"`
	IDC       string `json:"idc"`
}

var _ servicer.Servicer = new(CreateNodeInput)

func (input *CreateNodeInput) CheckParams(ctx *gin.Context) error {
	if input.GroupId <= 0 {
		return errors.New("invalid groupId")
	}
	if input.ServiceId <= 0 {
		return errors.New("invalid serviceId")
	}
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	if input.RegionId <= 0 {
		return errors.New("invalid regionId")
	}
	if input.CosFileId <= 0 {
		return errors.New("invalid packageId")
	}
	if len(input.MachineId) <= 0 && input.IDC == "" {
		return errors.New("invalid machineId and idc")
	}
	return nil
}

func (input *CreateNodeInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	r, err := tbl_region.GetInfo(input.RegionId)
	if err != nil {
		return nil, err
	}
	cosFileInfo, err := tbl_cosfile.GetCosFile(input.CosFileId)
	if err != nil {
		return nil, err
	}
	if cosFileInfo.ID <= 0 {
		return nil, errors.New("can't find cos file")
	}
	input.ServiceId = def.SERVICE_ID_BITALOS
	serviceInfo, err := tbl_service.GetInfo(input.ServiceId)
	if err != nil {
		log.Warnf("get service info failed.serviceId:%d", input.ServiceId)
		return nil, err
	}
	clusterInfo, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil {
		log.Warn("get cluster info failed.err:", err)
		return nil, err
	}
	regionId := clusterInfo.RegionId
	regionName := r.Name
	if r.NewId > 0 {
		regionId = r.NewId
		newInfo, err := tbl_region.GetInfo(r.NewId)
		if err != nil {
			return nil, err
		}
		regionName = newInfo.Name
	}

	var machineInfo []*tbl_machine.Machine
	if len(input.MachineId) <= 0 {
		machineIds, err := tbl_regionmachine.GetMachinesByRegion(regionId)
		if err != nil {
			log.Warn("get region machine info failed.err:", err)
			return nil, err
		}
		machineInfos, err := tbl_machine.GetOnlineList(machineIds, input.IDC)
		if err != nil {
			log.Warn("get machine info failed.err:", err)
			return nil, err
		}
		if len(machineInfos) == 0 {
			log.Warn("empty machine list")
			return nil, errors.New("invalid machineId idc param.")
		}
		isWitness := false
		if input.NodeRole == def.NODE_ROLE_WITNESS {
			isWitness = true
		}
		nodeList, err := tbl_node.GetMachinesOnlineNodes(machineIds, input.ServiceId, isWitness)
		if err != nil {
			return nil, errors.New("get node failed, err")
		}
		machineNodeMap := make(map[uint]int, len(machineIds))
		for i := 0; i < len(nodeList); i++ {
			machineNodeMap[nodeList[i].MachineId] += 1
		}

		onlineMachineList, err := tbl_node.GetOnlineClusterMachine(input.ClusterId, input.ServiceId, isWitness)
		if err != nil {
			return nil, errors.New("get online machine failed, err")
		}
		clusterMachine := make(map[uint]int, len(onlineMachineList))
		for _, onlineMachine := range onlineMachineList {
			clusterMachine[onlineMachine.MachineId] += 1
		}
		var uninstallNodeMachines []*tbl_machine.Machine
		for i := 0; i < len(machineInfos); i++ {
			if num, ok := clusterMachine[machineInfos[i].ID]; ok {
				machineInfos[i].NodeSum = uint(num)
			} else {
				if num, ok := machineNodeMap[machineInfos[i].ID]; ok {
					machineInfos[i].NodeSum = uint(num)
				}
				uninstallNodeMachines = append(uninstallNodeMachines, machineInfos[i])
			}
		}

		groupMachineList, err := tbl_node.GetNodesByStatus(def.NODE_STATUS_ONLINE, input.ClusterId, input.GroupId)
		if err != nil {
			return nil, errors.New("get online group machine failed, err")
		}
		groupMachines := make(map[uint]int, len(groupMachineList))
		for _, tmp := range groupMachineList {
			groupMachines[tmp.MachineId] = 1
		}
		if len(uninstallNodeMachines) <= 0 {
			sort.Sort(tbl_machine.MachinesSorter(machineInfos))
			for _, m := range machineInfos {
				if _, ok := groupMachines[m.ID]; !ok {
					machineInfo = append(machineInfo, m)
					break
				}
			}
			if machineInfo == nil {
				machineInfo = append(machineInfo, machineInfos[0])
			}
		} else {
			sort.Sort(tbl_machine.MachinesSorter(uninstallNodeMachines))
			machineInfo = append(machineInfo, uninstallNodeMachines[0])
		}
	} else {
		machineInfo, err = tbl_machine.GetList(input.MachineId)
		if err != nil {
			log.Warnf("get mdl_machine failed.err:%+v", err)
			return nil, err
		}
	}
	initRaft, groupInfo, err := mdl_group.GetGroupInfo(input.ClusterId, input.GroupId, input.ServiceId)
	if err != nil {
		log.Warn("get group info failed.err:", err)
		return nil, err
	}
	dashboardName, err := mdl_dashboard.GetDashboardName(clusterInfo.StoredId)
	if err != nil {
		return nil, err
	}

	deraftToken := math2.GetMd5(clusterInfo.Name)
	for _, machine := range machineInfo {
		nodeInfo := &tbl_node.Node{
			ClusterId:      input.ClusterId,
			GroupId:        input.GroupId,
			CosFileId:      input.CosFileId,
			CosFileVersion: cosFileInfo.Version,
			RegionId:       regionId,
			MachineId:      machine.ID,
			ServiceId:      input.ServiceId,
			IsWitness:      input.NodeRole == def.NODE_ROLE_WITNESS,
		}
		pod, err := tbl_node.Create(nodeInfo, groupInfo.MaxNodeId)
		if err != nil {
			continue
		}
		e := tbl_group.Update(clusterInfo.Id, input.GroupId, tbl_group.Group{
			MaxNodeId: pod.NodeId,
		})
		if e != nil {
			log.Errorf("update max_node_id fail.gid:%d maxNodeId:%d err:%v", input.GroupId, pod.NodeId, e)
		}
		task := &tbl_task.Task{
			Type:           def.TASK_TYPE_PREPAREADD,
			Status:         def.TASK_NEW,
			RegionId:       regionId,
			MachineId:      machine.ID,
			ServiceId:      input.ServiceId,
			ClusterId:      input.ClusterId,
			GroupId:        input.GroupId,
			NodeId:         pod.NodeId,
			CosFileId:      input.CosFileId,
			CosFileVersion: cosFileInfo.Version,
			TaskExt: tbl_task.TaskExtra{
				Ip:               machine.IP,
				RegionName:       regionName,
				ServiceName:      serviceInfo.Name,
				HostName:         machine.HostName,
				ServicePortRange: mdl_port.NarrowDownPortRange(serviceInfo.PortRanges, machine.ID),
				ClusterPortRange: mdl_port.NarrowDownPortRange(serviceInfo.ClusterPortRanges, machine.ID),
				ClusterName:      clusterInfo.Name,
				DashboardName:    dashboardName,
				DashboardAddress: config.GetConf().Domains.Dashboard,
				CloudType:        machine.IDC,
				Operation:        def.OPERATION_SUPERVISOR_START,
				IsWitness:        input.NodeRole == def.NODE_ROLE_WITNESS,
				IsObserver:       input.NodeRole == def.NODE_ROLE_OBSERVER,
				NodeList:         initRaft,
				NodeIdList:       groupInfo.InitNodeId,
				NodeListStr:      groupInfo.InitRaft,
				NodeIndex:        int(pod.NodeId),
				DeraftToken:      deraftToken,
				UpdateConfig:     true,
			},
		}
		err = tbl_task.CreateTask(task)
		if err != nil {
			log.Warn("create task info failed.err:", err)
		}
		log.Infof("add task:%+v", task)
	}
	return nil, nil
}

type CreateNodeOutput struct {
	NodeId  uint `json:"nodeId"`
	GroupId uint `json:"groupId"`
}
