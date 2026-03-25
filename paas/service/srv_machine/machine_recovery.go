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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_group"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/model/redis_op"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"strconv"
)

type MachineRecoveryInput struct {
	MachineId uint `form:"machineId"`
}

var _ servicer.Servicer = new(MachineRecoveryInput)

func (input *MachineRecoveryInput) CheckParams(ctx *gin.Context) error {
	if input.MachineId == 0 {
		return errors.New("invalid machineId")
	}
	return nil
}

func (input *MachineRecoveryInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	mInfo, err := tbl_machine.GetInfo(input.MachineId)
	if err != nil {
		log.Warn("get machine info failed.err:", err)
		return nil, err
	}
	if mInfo.ID == 0 {
		log.Warn("invalid machine id:", input.MachineId)
		return nil, err
	}
	if mInfo.Status == def.MACHINE_STATUS_OFFLINE {
		return nil, errors.New("can only recovery a online machine.")
	}
	nodeList, err := tbl_node.GetOnlineNodes(input.MachineId)
	if err != nil {
		log.Warn("get machine nodelist failed.err:", err)
		return nil, err
	}
	for _, nodeInfo := range nodeList {
		clusterInfo, err := tbl_cluster.GetInfo(nodeInfo.ClusterId)
		if err != nil {
			log.Warnf("recovery failed clusterId:%d serviceId:%d groupId:%d nodeId:%d get cluster info failed.err:%v", nodeInfo.ClusterId,
				nodeInfo.ServiceId, nodeInfo.GroupId, nodeInfo.NodeId, err)
			continue
		}
		isAlive := redis_op.IsOnline(mInfo.IP+":"+strconv.Itoa(int(nodeInfo.ServicePort)), clusterInfo.Id)
		if isAlive {
			log.Warnf("recovery failed clusterId:%d serviceId:%d groupId:%d nodeId:%d node is online", nodeInfo.ClusterId,
				nodeInfo.ServiceId, nodeInfo.GroupId, nodeInfo.NodeId)
			continue
		}
		regionInfo, err := tbl_region.GetInfo(nodeInfo.RegionId)
		if err != nil {
			log.Warnf("recovery failed clusterId:%d serviceId:%d groupId:%d nodeId:%d get region info failed.err:%v", nodeInfo.ClusterId,
				nodeInfo.ServiceId, nodeInfo.GroupId, nodeInfo.NodeId, err)
			continue
		}
		regionName := regionInfo.Name
		serviceInfo, err := tbl_service.GetInfo(nodeInfo.ServiceId)
		if err != nil {
			log.Warnf("recovery failed clusterId:%d serviceId:%d groupId:%d nodeId:%d get service info failed.err:%v", nodeInfo.ClusterId,
				nodeInfo.ServiceId, nodeInfo.GroupId, nodeInfo.NodeId, err)
			continue
		}
		initRaft, groupInfo, err := mdl_group.GetGroupInfo(nodeInfo.ClusterId, nodeInfo.GroupId, nodeInfo.ServiceId)
		if err != nil {
			log.Warnf("recovery failed clusterId:%d serviceId:%d groupId:%d nodeId:%d get cluster info failed.err:%v", nodeInfo.ClusterId,
				nodeInfo.ServiceId, nodeInfo.GroupId, nodeInfo.NodeId, err)
			continue
		}
		dashboardName, err := mdl_dashboard.GetDashboardName(clusterInfo.StoredId)
		if err != nil {
			log.Warn("get dashboard name failed.err:", err)
			dashboardName = clusterInfo.Name
		}
		witness := false
		if nodeInfo.IsWitness {
			witness = true
		}
		operation := def.OPERATION_SUPERVISOR_START
		var extString string
		if nodeInfo.ServiceId == def.SERVICE_ID_DASHBOARD || nodeInfo.ServiceId == def.SERVICE_ID_FE {
			operation = def.OPERATION_START
		} else {
			extString = mdl_resource_pool.FormatMachineCgroup(nodeInfo.ServicePort, clusterInfo.Id, mInfo.ID, mInfo.IDC)
		}
		task := &tbl_task.Task{
			Type:      def.TASK_TYPE_START,
			Status:    def.TASK_NEW,
			RegionId:  nodeInfo.RegionId,
			MachineId: input.MachineId,
			ServiceId: nodeInfo.ServiceId,
			ClusterId: nodeInfo.ClusterId,
			GroupId:   nodeInfo.GroupId,
			NodeId:    nodeInfo.NodeId,
			CosFileId: nodeInfo.CosFileId,
			TaskExt: tbl_task.TaskExtra{
				Ip:               mInfo.IP,
				RegionName:       regionName,
				ServiceName:      serviceInfo.Name,
				HostName:         mInfo.HostName,
				ServicePort:      nodeInfo.ServicePort,
				ClusterPort:      nodeInfo.ClusterPort,
				ClusterName:      clusterInfo.Name,
				DashboardName:    dashboardName,
				DashboardAddress: config.GetConf().Domains.Dashboard,
				CloudType:        mInfo.IDC,
				NodeIndex:        int(nodeInfo.NodeId),
				NodeList:         initRaft,
				NodeIdList:       groupInfo.InitNodeId,
				NodeListStr:      groupInfo.InitRaft,
				NodeListVal:      fmt.Sprintf("%s:%d", mInfo.IP, nodeInfo.ClusterPort),
				Operation:        operation,
				IsWitness:        witness,
				IsObserver:       false,
				DeraftToken:      math2.GetMd5(clusterInfo.Name),
				StoredAuth:       "",
				UpdateConfig:     false,
				ExtString:        extString,
			},
		}
		err = tbl_task.CreateTask(task)
		if err != nil {
			log.Warnf("recovery failed clusterId:%d serviceId:%d groupId:%d nodeId:%d create task failed.err:%v", nodeInfo.ClusterId,
				nodeInfo.ServiceId, nodeInfo.GroupId, nodeInfo.NodeId, err)
			continue
		}
		log.Infof("node operation task:%+v", task)
	}

	return nil, nil
}
