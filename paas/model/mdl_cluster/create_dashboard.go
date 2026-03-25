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

package mdl_cluster

import (
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_config"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cosfile"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_group"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_hostport"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_machine"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_port"
	"github.com/zuoyebang/bitalostored/paas/model/strategy"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"github.com/zuoyebang/bitalostored/paas/utils/rpc"
)

type CreateDashboardModelInput struct {
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
	Department   string `json:"department"`
}

type CreateProxyModelInput struct {
	RegionId          uint   `json:"regionId"`
	ServiceId         uint   `json:"serviceId"`
	ConfigPackId      uint   `json:"configPackId"`
	CosFileId         uint   `json:"packageId"`
	ClusterName       string `json:"clusterName"`
	Department        string `json:"department"`
	StoredId          uint   `json:"storedId"`
	AssignedPort      uint   `json:"assignedPort"`
	AssignedAdminPort uint   `json:"assignedAdminPort"`
	Operation         string `json:"operation"`
	IDC               string `json:"idc"`
	IpList            string `json:"ipList"`
	Ips               []string
}

type CreateServerModelInput struct {
	RegionId     uint   `json:"regionId"`
	ServiceId    uint   `json:"serviceId"`
	CosFileId    uint   `json:"packageId"`
	ConfigPackId uint   `json:"configPackId"`
	ClusterName  string `json:"clusterName"`
	Department   string `json:"department"`
	StoredId     uint   `json:"storedId"` // Dashboard cluster ID
	Operation    string `json:"operation"`
	Ips          []string
}

func CreateClusterForDashboard(input *CreateDashboardModelInput) (*tbl_cluster.Cluster, error) {
	serviceInfo, regionInfo, clusterInfo, err := GetBasicInfo(input.ServiceId, input.RegionId, input.StoredId, input.ConfigPackId, input.ClusterName, input.Department)
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
	if err != nil {
		log.Warn("update task info failed.err:", err)
	}
	log.Infof("add task:%+v", task)
	return clusterInfo, err
}

func CreateProxyCluster(input *CreateProxyModelInput) (interface{}, error) {
	serviceInfo, regionInfo, clusterInfo, err := GetBasicInfo(input.ServiceId, input.RegionId, input.StoredId, input.ConfigPackId, input.ClusterName, input.Department)
	if err != nil {
		return nil, err
	}
	err = tbl_config.UpdateClusterId(input.ConfigPackId, clusterInfo.Id, input.ServiceId)
	if err != nil {
		return nil, err
	}

	// cgroup
	idcs := []string{def.IdcTxcloud, def.IdcAli}
	for _, idc := range idcs {
		if _, e := tbl_resource_pool.Create(input.ClusterName, def.CGROUP_NAME_CPU, idc, clusterInfo.Id, input.ServiceId, input.AssignedPort, def.DEFAULT_CGROUP_CPU); e != nil {
			log.Warnf("create resource pool error. cluster:%s", input.ClusterName)
		}
	}

	cosFile, err := tbl_cosfile.GetCosFile(input.CosFileId)
	if err != nil {
		return nil, err
	}
	group, err := tbl_group.Create(clusterInfo.Id, serviceInfo.ID)
	if err != nil {
		return nil, err
	}
	if err := mdl_dashboard.IsStoredSameName(input.StoredId, input.ClusterName); err != nil {
		log.Error("get stored cluster failed.err:", err)
		return "", err
	}
	machineIpInfos, err := mdl_machine.GetMachinesByIps(input.Ips)
	if err != nil {
		return nil, err
	}

	for _, machine := range machineIpInfos {
		if _, err := tbl_hostport.Create(machine.ID, input.AssignedPort, machine.IP); err != nil {
			msg := fmt.Sprintf("[%s:%d]create proxy failed，err: %s", machine.IP, input.AssignedAdminPort, err)
			log.Errorf(msg)
			rpc.SendDingding(rpc.OpErrTitle, msg)
			continue
		}
		if _, err := tbl_hostport.Create(machine.ID, input.AssignedAdminPort, machine.IP); err != nil {
			msg := fmt.Sprintf("[%s:%d]create proxy failed，err: %s", machine.IP, input.AssignedAdminPort, err)
			log.Errorf(msg)
			rpc.SendDingding(rpc.OpErrTitle, msg)
			continue
		}
		nodeInfo := &tbl_node.Node{
			ClusterId:      clusterInfo.Id,
			GroupId:        group.GroupId,
			CosFileId:      input.CosFileId,
			CosFileVersion: cosFile.Version,
			RegionId:       input.RegionId,
			MachineId:      machine.ID,
			ServiceId:      input.ServiceId,
			ServicePort:    input.AssignedPort,
			ClusterPort:    input.AssignedAdminPort,
		}
		node, err := tbl_node.Create(nodeInfo, 0)
		if err != nil {
			log.Warn("create node failed.err:", err)
			return nil, err
		}

		task := &tbl_task.Task{
			Type:           def.TASK_TYPE_START,
			Status:         def.TASK_NEW,
			RegionId:       input.RegionId,
			MachineId:      machine.ID,
			ServiceId:      input.ServiceId,
			ClusterId:      clusterInfo.Id,
			GroupId:        group.GroupId,
			NodeId:         node.NodeId,
			CosFileId:      input.CosFileId,
			CosFileVersion: cosFile.Version,
			TaskExt: tbl_task.TaskExtra{
				Ip:               machine.IP,
				RegionName:       regionInfo.Name,
				ServiceName:      serviceInfo.Name,
				ServicePort:      input.AssignedPort,
				ClusterPort:      input.AssignedAdminPort,
				ClusterName:      clusterInfo.Name,
				CloudType:        machine.IDC,
				Operation:        input.Operation,
				StoredAuth:       "",
				DashboardAddress: config.GetConf().Domains.Dashboard,
				UpdateConfig:     true,
			},
		}
		err = tbl_task.CreateTask(task)
		log.Infof("add task:%+v", task)
		if err != nil {
			log.Warn("update task info failed.err:", err)
			continue
		}
	}
	return nil, nil
}

func CreateServerCluster(input *CreateServerModelInput) (interface{}, error) {
	input.ServiceId = def.SERVICE_ID_BITALOS
	var err error
	machineMapList, err := strategy.FormatMachines(input.RegionId, input.ServiceId, 0, input.Ips, false)
	if err != nil {
		log.Warn("get format machine list failed.err:", err)
		return nil, err
	}
	if len(machineMapList) == 0 {
		return nil, errors.New("there is not enough machine")
	}
	cosFileInfo, err := tbl_cosfile.GetCosFile(input.CosFileId)
	if err != nil {
		return nil, err
	}
	if cosFileInfo.ID <= 0 {
		return nil, errors.New("can't find cos file")
	}
	if err := mdl_dashboard.IsStoredSameName(input.StoredId, input.ClusterName); err != nil {
		log.Error("get stored cluster failed.err:", err)
		return "", err
	}

	groupNum := 1
	groupMachines, err := strategy.SplitMachinesByGroup("", groupNum, len(input.Ips), machineMapList)
	if err != nil {
		log.Warn("get group machine list failed.err:", err)
		return nil, err
	}

	serviceInfo, regionInfo, clusterInfo, err := GetBasicInfo(input.ServiceId, input.RegionId, input.StoredId, input.ConfigPackId, input.ClusterName, input.Department)
	if err != nil {
		log.Warn("get basic info failed.err:", err)
		return nil, err
	}

	idcs := []string{def.IdcTxcloud, def.IdcAli}
	for _, idc := range idcs {
		if _, e := tbl_resource_pool.Create(input.ClusterName, def.CGROUP_NAME_CPU, idc, clusterInfo.Id, input.ServiceId, 0, def.DEFAULT_CGROUP_CPU); e != nil {
			log.Warnf("create resource pool error %v. cluster:%s", e, input.ClusterName)
			msg := fmt.Sprintf("create cluster %s set resource failed", input.ClusterName)
			e := rpc.SendDingding(rpc.OpErrTitle, msg)
			if e != nil {
				log.Warnf("send dingding fail err:%v", e)
			}
		}
	}

	err = tbl_config.UpdateClusterId(input.ConfigPackId, clusterInfo.Id, input.ServiceId)
	if err != nil {
		return nil, err
	}

	for _, ms := range groupMachines {
		grp, err := tbl_group.Create(clusterInfo.Id, serviceInfo.ID)
		if err != nil {
			return nil, errors.New("create group failed")
		}
		var maxNodeId uint
		for _, m := range ms {
			nodeInfo := &tbl_node.Node{
				ClusterId:      clusterInfo.Id,
				GroupId:        grp.GroupId,
				CosFileId:      input.CosFileId,
				CosFileVersion: cosFileInfo.Version,
				RegionId:       input.RegionId,
				MachineId:      m.ID,
				ServiceId:      input.ServiceId,
				IsWitness:      false,
			}
			pod, err := tbl_node.Create(nodeInfo, 0)
			if err != nil {
				log.Warn("create node failed.err:", err)
				return nil, err
			}
			maxNodeId = pod.NodeId
			task := &tbl_task.Task{
				Type:           def.TASK_TYPE_PREPARESTART,
				Status:         def.TASK_NEW,
				RegionId:       input.RegionId,
				MachineId:      m.ID,
				ServiceId:      input.ServiceId,
				ClusterId:      clusterInfo.Id,
				GroupId:        grp.GroupId,
				NodeId:         pod.NodeId,
				CosFileId:      input.CosFileId,
				CosFileVersion: cosFileInfo.Version,
				TaskExt: tbl_task.TaskExtra{
					Ip:               m.IP,
					RegionName:       regionInfo.Name,
					ServiceName:      serviceInfo.Name,
					HostName:         m.HostName,
					ServicePortRange: mdl_port.NarrowDownPortRange(serviceInfo.PortRanges, m.ID),
					ClusterPortRange: mdl_port.NarrowDownPortRange(serviceInfo.ClusterPortRanges, m.ID),
					ClusterName:      input.ClusterName,
					DashboardName:    input.ClusterName,
					CloudType:        m.IDC,
					NodeIndex:        int(pod.NodeId),
					Operation:        input.Operation,
					DeraftToken:      math2.GetMd5(clusterInfo.Name),
					UpdateConfig:     true,
				},
			}
			err = tbl_task.CreateTask(task)
			log.Infof("add task:%+v", task)
			if err != nil {
				log.Warn("create task info failed.err:", err)
				return nil, err
			}
		}
		if maxNodeId > 0 {
			e := tbl_group.Update(clusterInfo.Id, grp.GroupId, tbl_group.Group{
				MaxNodeId: maxNodeId,
			})
			if e != nil {
				log.Errorf("update max_node_id fail.gid:%d maxNodeId:%d err:%v", grp.GroupId, maxNodeId, e)
				msg := fmt.Sprintf("update max nodeId failed, clusterId:%d groupId:%d maxNodeId:%d", clusterInfo.Id, grp.GroupId, maxNodeId)
				e := rpc.SendDingding(rpc.OpErrTitle, msg)
				if e != nil {
					log.Warnf("send dingding fail err:%v", e)
				}
			}
		}
	}

	return nil, err
}
