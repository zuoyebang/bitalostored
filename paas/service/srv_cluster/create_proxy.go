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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_cluster"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_machine"
	"github.com/zuoyebang/bitalostored/paas/model/redis_op"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

type CreateProxyInput struct {
	RegionId          uint   `json:"regionId"`
	ServiceId         uint   `json:"serviceId"`
	ConfigPackId      uint   `json:"configPackId"`
	CosFileId         uint   `json:"packageId"`
	ClusterName       string `json:"clusterName"`
	StoredId          uint   `json:"storedId"`
	AssignedPort      uint   `json:"assignedPort"`
	AssignedAdminPort uint   `json:"assignedAdminPort"`
	Operation         string `json:"operation"`
	//Num               uint   `json:"nodeNum"`
	IDC    string `json:"idc"`
	IpList string `json:"ipList"`
	Ips    []string
}

var _ servicer.Servicer = new(CreateProxyInput)

func (input *CreateProxyInput) CheckParams(ctx *gin.Context) error {
	if input.RegionId <= 0 {
		return errors.New("invalid regionId")
	}
	if input.ServiceId <= 0 {
		return errors.New("invalid serviceId")
	}
	if input.StoredId <= 0 {
		return errors.New("invalid storedId")
	}
	if input.CosFileId <= 0 {
		return errors.New("invalid cosFileId")
	}
	if input.AssignedPort <= 0 || input.AssignedAdminPort <= 0 {
		return errors.New("invalid port")
	}
	if input.ConfigPackId <= 1 {
		return errors.New("invalid configPackId. Do not use the default config")
	}
	input.ClusterName = strings.TrimSpace(input.ClusterName)
	if input.ClusterName <= "" {
		return errors.New("invalid clusterName")
	}
	if input.IDC == "" {
		return errors.New("invalid idc")
	}
	if len(input.IpList) <= 0 {
		return errors.New("empty ipList")
	}
	ips := strings.Split(input.IpList, "\n")
	if len(ips) <= 0 {
		return errors.New("invalid ip")
	}
	input.Ips = ips
	return nil
}

func (input *CreateProxyInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	taskNum := 0
	serviceInfo, regionInfo, clusterInfo, err := mdl_cluster.GetBasicInfo(input.ServiceId, input.RegionId, input.StoredId, input.ConfigPackId, input.ClusterName, "")
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
			isAlive := redis_op.IsOnline(machine.IP+":"+strconv.Itoa(int(input.AssignedPort)), clusterInfo.Id)
			if isAlive {
				continue
			}
		}
		if _, err := tbl_hostport.Create(machine.ID, input.AssignedAdminPort, machine.IP); err != nil {
			isAlive := redis_op.IsOnline(machine.IP+":"+strconv.Itoa(int(input.AssignedAdminPort)), clusterInfo.Id)
			if isAlive {
				continue
			}
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
		taskNum++
	}

	return CreateClusterOutput{ClusterId: clusterInfo.Id, TaskCreated: taskNum}, err
}
