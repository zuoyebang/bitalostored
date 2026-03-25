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
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/dao/web/dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_group"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_ops"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/model/redis_op"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

type OperateNodeInput struct {
	ClusterId uint   `json:"clusterId"`
	GroupId   uint   `json:"groupId"`
	NodeId    uint   `json:"nodeId"`
	Operation string `json:"operation"`
	CosFileId uint   `json:"cosFileId"`
	Remove    int    `json:"remove"`
}

var _ servicer.Servicer = new(OperateNodeInput)

func (input *OperateNodeInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	if input.GroupId <= 0 {
		return errors.New("invalid groupId")
	}
	if input.NodeId <= 0 {
		return errors.New("invalid nodeId")
	}
	if input.Operation == "" {
		return errors.New("invalid operation")
	}
	return nil
}

func (input *OperateNodeInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	nodeInfo, err := tbl_node.GetInfo(input.NodeId, input.GroupId, input.ClusterId)
	if err != nil {
		return nil, err
	}
	machineInfo, err := tbl_machine.GetInfo(nodeInfo.MachineId)
	if err != nil {
		return nil, err
	}
	address := machineInfo.IP + ":" + strconv.Itoa(int(nodeInfo.ServicePort))
	clusterInfo, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil {
		return nil, err
	}
	serviceInfo, err := tbl_service.GetInfo(nodeInfo.ServiceId)
	if err != nil {
		return nil, err
	}
	regionInfo, err := tbl_region.GetInfo(nodeInfo.RegionId)
	if err != nil {
		return nil, err
	}
	regionName := regionInfo.Name
	/*
		initTasks, err := tbl_task.GetInitTask(nodeInfo.ClusterId, nodeInfo.GroupId, nodeInfo.MachineId, nodeInfo.NodeId)
		if err != nil {
			return nil, err
		}
		if len(initTasks) <= 0 {
			return nil, errors.New("missing success start task")
		}
		regionName := initTasks[0].TaskExt.RegionName
		if len(regionName) <= 0 {
			regionInfo, err := tbl_region.GetInfo(nodeInfo.RegionId)
			if err != nil {
				return nil, err
			}
			regionName = regionInfo.Name
		}

	*/
	dashboardName, err := mdl_dashboard.GetDashboardName(clusterInfo.StoredId)
	if err != nil {
		log.Warn("get dashboard name failed.err:", err)
		dashboardName = clusterInfo.Name
	}
	operation := input.Operation
	if def.IsServer(nodeInfo.ServiceId) && operation != def.OPERATION_SUPERVISOR_START {
		infos, err := redis_op.GetInfo(address, input.ClusterId, input.GroupId, clusterInfo.Name)
		if err != nil {
			log.Warnf("get info fail, err:%v", err.Error())
			return nil, errors.New("could not shutdown error node")
		}
		replicas, err := GetReplica(dashboardName)
		if err != nil {
			log.Warn("get replica failed.err:", err)
			return nil, err
		}
		replica := replicas[address]
		if replica {
			return nil, errors.New("need replica")
		}
		replicaGroupIds, err := mdl_node.GetSlotsGroupIds(clusterInfo.Name, def.SERVICE_ID_PROXY)
		if err != nil {
			return nil, err
		}
		if _, ok := replicaGroupIds[nodeInfo.GroupId]; ok {

			role, ok := infos["role"]
			if ok && role == def.NODE_ROLE_MASTER {
				return nil, errors.New("could not shutdown master node")
			}
			if input.Operation == def.OPERATION_SUPERVISOR_STOP {
				if status, ok := infos["status"]; ok {
					if status == "true" {
						return nil, errors.New("could not shutdown clusterStatus true node")
					}
				}
			}
		}
	}
	var initRaft []string
	var initRaftStr, initNodeIds string
	if def.IsServer(nodeInfo.ServiceId) {
		raft, groupInfo, err := mdl_group.GetGroupInfo(nodeInfo.ClusterId, nodeInfo.GroupId, nodeInfo.ServiceId)
		if err != nil {
			log.Warnf("clusterId:%d groupId:%d nodeId:%d get group fail err=%v", input.ClusterId, input.GroupId, input.NodeId, err)
			return nil, err
		}
		initRaftStr = groupInfo.InitRaft
		initNodeIds = groupInfo.InitNodeId
		initRaft = raft
	}
	taskType := def.TASK_TYPE_OPERATE
	if input.Operation == def.OPERATION_BITALOS_UPGRADE {
		taskType = def.TASK_TYPE_UPGRADE
	}

	if nodeInfo.ServiceId == def.SERVICE_ID_PROXY && input.Operation == def.OPERATION_RESTART {
		operation = def.OPERATION_STOP
	}
	var extString string
	if input.Operation == def.OPERATION_SUPERVISOR_START {
		extString = mdl_resource_pool.FormatMachineCgroup(nodeInfo.ServicePort, clusterInfo.Id, machineInfo.ID, machineInfo.IDC)
	}
	task := &tbl_task.Task{
		Type:           taskType,
		Status:         def.TASK_NEW,
		RegionId:       nodeInfo.RegionId,
		MachineId:      machineInfo.ID,
		ServiceId:      nodeInfo.ServiceId,
		ClusterId:      input.ClusterId,
		GroupId:        input.GroupId,
		NodeId:         input.NodeId,
		CosFileId:      input.CosFileId,
		CosFileVersion: nodeInfo.CosFileVersion,
		TaskExt: tbl_task.TaskExtra{
			Ip:               machineInfo.IP,
			RegionName:       regionName,
			ServiceName:      serviceInfo.Name,
			HostName:         machineInfo.HostName,
			ServicePort:      nodeInfo.ServicePort,
			ClusterPort:      nodeInfo.ClusterPort,
			ClusterName:      clusterInfo.Name,
			DashboardName:    dashboardName,
			DashboardAddress: config.GetConf().Domains.Dashboard,
			CloudType:        machineInfo.IDC,
			NodeIndex:        int(input.NodeId),
			NodeList:         initRaft,
			NodeIdList:       initNodeIds,
			NodeListStr:      initRaftStr,
			NodeListVal:      fmt.Sprintf("%s:%d", machineInfo.IP, nodeInfo.ClusterPort),
			Operation:        operation,
			IsWitness:        nodeInfo.IsWitness,
			IsObserver:       false,
			DeraftToken:      math2.GetMd5(clusterInfo.Name),
			StoredAuth:       "",
			UpdateConfig:     false,
			ExtString:        extString,
		},
	}
	err = tbl_task.CreateTask(task)
	if err != nil {
		log.Warn("create task failed.err:", err)
		return nil, err
	}
	mdl_ops.CreateOpsActionLog(nodeInfo, task)
	log.Infof("node operation task:%+v", task)
	if def.IsServer(nodeInfo.ServiceId) && operation == def.OPERATION_SUPERVISOR_STOP && input.Remove == 1 {
		go func() {
			nodeIsDown := false
			serverAddr := fmt.Sprintf("%s:%d", machineInfo.IP, nodeInfo.ServicePort)
			clusterName := clusterInfo.Name
			groupId := nodeInfo.GroupId
			// Check node is not connected, which means the node is down.
			for i := 0; i < 10; i++ {
				if !redis_op.IsOnline(serverAddr, nodeInfo.ClusterId) {
					nodeIsDown = true
					break
				}
				time.Sleep(3 * time.Second)
			}

			if nodeIsDown {
				// Purpose: Remove it from dashboard
				if err := dashboard.SetDashboardCookie(ctx); err != nil {
					log.Warnf("cluster:%s set dashboard cookie failed.err=%v", clusterInfo.Name, err)
					return
				}
				if err := dashboard.DelServerNode(ctx, clusterName, groupId, serverAddr, int(nodeInfo.NodeId)); err != nil {
					log.Errorf("delete server false. cluster:%s node:%s error:%s", clusterName, serverAddr, err)
				}
			}
		}()
	}
	return nil, nil
}
