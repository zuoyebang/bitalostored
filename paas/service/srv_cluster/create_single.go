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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_config"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cosfile"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_group"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_hostport"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_cluster"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type createSingleInput struct {
	RegionId     uint   `json:"regionId"`
	ServiceId    uint   `json:"serviceId"`
	CosFileId    uint   `json:"packageId"`
	ConfigPackId uint   `json:"configPackId"`
	ClusterName  string `json:"clusterName"`
	AssignedPort uint   `json:"assignedPort"`
	StoredId     uint   `json:"storedId"`
	MachineId    uint   `json:"machineId"`
	Operation    string `json:"operation"`
	StoredAuth   string `json:"storedAuth"`
	Role         string `json:"role"`
}

func (input *createSingleInput) createSingle() (interface{}, error) {
	taskNum := 0
	serviceInfo, regionInfo, clusterInfo, err := mdl_cluster.GetBasicInfo(input.ServiceId, input.RegionId, input.StoredId, input.ConfigPackId, input.ClusterName, "")
	if err != nil {
		log.Warn("get basic info failed.err:", err)
		return nil, err
	}
	group, err := tbl_group.Create(clusterInfo.Id, serviceInfo.ID)
	if err != nil {
		log.Warn("create group failed.err:", err)
		return nil, err
	}
	cosFile, err := tbl_cosfile.GetCosFile(input.CosFileId)
	if err != nil {
		log.Warn("get cosFile failed.err:", err)
		return nil, err
	}
	machineInfo, err := tbl_machine.GetInfo(input.MachineId)
	if err != nil {
		log.Warn("get machine info failed.err:", err)
		return nil, err
	}
	if _, err := tbl_hostport.Create(machineInfo.ID, input.AssignedPort, machineInfo.IP); err != nil {
		log.Warn("assign host port failed.err:", err)
		return nil, err
	}
	nodeInfo := &tbl_node.Node{
		ClusterId:      clusterInfo.Id,
		GroupId:        group.GroupId,
		CosFileId:      cosFile.ID,
		CosFileVersion: cosFile.Version,
		RegionId:       input.RegionId,
		MachineId:      input.MachineId,
		ServiceId:      input.ServiceId,
		ServicePort:    input.AssignedPort,
	}
	node, err := tbl_node.Create(nodeInfo, 0)
	if err != nil {
		log.Warn("create node failed.err:", err)
		return nil, err
	}
	err = tbl_config.UpdateClusterId(input.ConfigPackId, clusterInfo.Id, input.ServiceId)
	if err != nil {
		return nil, err
	}
	task := &tbl_task.Task{
		Type:           def.TASK_TYPE_START,
		Status:         def.TASK_NEW,
		RegionId:       input.RegionId,
		MachineId:      input.MachineId,
		ServiceId:      input.ServiceId,
		ClusterId:      clusterInfo.Id,
		GroupId:        group.GroupId,
		NodeId:         node.NodeId,
		CosFileId:      input.CosFileId,
		CosFileVersion: cosFile.Version,
		TaskExt: tbl_task.TaskExtra{
			Ip:           machineInfo.IP,
			RegionName:   regionInfo.Name,
			ServiceName:  serviceInfo.Name,
			ServicePort:  input.AssignedPort,
			ClusterName:  input.ClusterName,
			CloudType:    machineInfo.IDC,
			Operation:    input.Operation,
			StoredAuth:   input.StoredAuth,
			UpdateConfig: true,
		},
	}
	err = tbl_task.CreateTask(task)
	taskNum++
	if err != nil {
		log.Warn("update task info failed.err:", err)
		taskNum--
	}
	log.Infof("add task:%+v", task)

	return CreateSingleOutput{ClusterId: clusterInfo.Id, TaskCreated: taskNum, StoredId: clusterInfo.StoredId}, err
}
