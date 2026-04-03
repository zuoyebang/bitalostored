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
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/redis_cli"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_group"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/dao/web/dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_group"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_ops"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/model/redis_op"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"github.com/zuoyebang/bitalostored/paas/utils/toolkit"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
)

type UpgradeAttr struct {
	ClusterName string
	OpAction    string
	NodeMsg     string
	ServiceId   uint
}

func upgradeNormalNode(clusterId, nodeId, cosFileId uint, operation, updateConfig string) (*UpgradeAttr, error) {
	var groupId uint = 1
	retAttr := &UpgradeAttr{}
	nodeInfo, err := tbl_node.GetInfo(nodeId, groupId, clusterId)
	if err != nil {
		log.Errorf("get node info id:%d failed.err:%v", nodeId, err)
		return nil, err
	}
	retAttr.ServiceId = nodeInfo.ServiceId
	if nodeInfo.Status == def.NODE_STATUS_OFFLINE {
		return nil, errors.New("upgrading an offline node. Please check and retry")
	}
	machineInfo, err := tbl_machine.GetInfo(nodeInfo.MachineId)
	if err != nil {
		log.Errorf("get machine info id:%d failed.err:%v", nodeInfo.MachineId, err)
		return nil, err
	}
	serviceName := def.GetServiceNameFromServiceId(int(nodeInfo.ServiceId))
	regionInfo, err := tbl_region.GetInfo(nodeInfo.RegionId)
	if err != nil {
		return nil, err
	}
	regionName := regionInfo.Name
	/*
		taskInfos, err := tbl_task.GetInitTask(nodeInfo.ClusterId, nodeInfo.GroupId, nodeInfo.MachineId, nodeInfo.NodeId)
		if err != nil || len(taskInfos) == 0 {
			return nil, err
		}
		taskInfo := taskInfos[0]
		regionName := taskInfo.TaskExt.RegionName
		if len(regionName) <= 0 {
			regionInfo, err := tbl_region.GetInfo(nodeInfo.RegionId)
			if err != nil {
				return nil, err
			}
			regionName = regionInfo.Name
		}

	*/
	//log.Infof("model task:%+v", taskInfo)
	clusterInfo, err := tbl_cluster.GetInfo(clusterId)
	if err != nil {
		return nil, err
	}
	retAttr.ClusterName = clusterInfo.Name
	dashboardName, err := mdl_dashboard.GetDashboardName(clusterInfo.StoredId)
	if err != nil {
		return nil, err
	}
	if nodeInfo.ServiceId == def.SERVICE_ID_PROXY && operation == def.OPERATION_RESTART {
		operation = def.OPERATION_STOP
		retAttr.OpAction = "proxy reload"
		retAttr.NodeMsg = fmt.Sprintf("%s:%d", machineInfo.IP, nodeInfo.ServicePort)
	}
	var extString string
	if operation == def.OPERATION_SUPERVISOR_START {
		extString = mdl_resource_pool.FormatMachineCgroup(nodeInfo.ServicePort, clusterInfo.Id, machineInfo.ID, machineInfo.IDC)
	}
	task := &tbl_task.Task{
		Type:      def.TASK_TYPE_UPGRADE,
		Status:    def.TASK_NEW,
		RegionId:  nodeInfo.RegionId,
		MachineId: machineInfo.ID,
		ServiceId: nodeInfo.ServiceId,
		ClusterId: clusterId,
		GroupId:   groupId,
		NodeId:    nodeId,
		CosFileId: cosFileId,
		TaskExt: tbl_task.TaskExtra{
			Ip:               machineInfo.IP,
			RegionName:       regionName,
			ServiceName:      serviceName,
			HostName:         machineInfo.HostName,
			ServicePort:      nodeInfo.ServicePort,
			ClusterPort:      nodeInfo.ClusterPort,
			ClusterName:      clusterInfo.Name,
			DashboardName:    dashboardName,
			DashboardAddress: config.GetConf().Domains.Dashboard,
			CloudType:        machineInfo.IDC,
			NodeIndex:        int(nodeId),
			Operation:        operation,
			DeraftToken:      math2.GetMd5(clusterInfo.Name),
			StoredAuth:       "",
			UpdateConfig:     updateConfig != "false",
			ExtString:        extString,
		},
	}
	err = tbl_task.CreateTask(task)
	if err != nil {
		log.Warn("create task failed.err:", err)
		return nil, err
	}
	mdl_ops.CreateOpsActionLog(nodeInfo, task)
	return retAttr, nil
}

func upgradeServerNode(ctx *gin.Context, input *UpgradeNodeInput) (interface{}, error) {
	nodeInfo, err := tbl_node.GetInfo(input.NodeId, input.GroupId, input.ClusterId)
	if err != nil {
		log.Errorf("get node info id:%d failed.err:%v", input.NodeId, err)
		return nil, err
	}
	if nodeInfo.Status == def.NODE_STATUS_OFFLINE {
		return nil, errors.New("upgrading an offline node. Please check and retry")
	}
	machineInfo, err := tbl_machine.GetInfo(nodeInfo.MachineId)
	if err != nil {
		log.Errorf("get machine info id:%d failed.err:%v", nodeInfo.MachineId, err)
		return nil, err
	}
	address := machineInfo.IP + ":" + strconv.Itoa(int(nodeInfo.ServicePort))
	serviceInfo, err := tbl_service.GetInfo(nodeInfo.ServiceId)
	if err != nil {
		log.Errorf("get service info id:%d failed.err:%v", nodeInfo.ServiceId, err)
		return nil, err
	}
	taskInfos, err := tbl_task.GetClusterTask(nodeInfo.ClusterId)
	if err != nil || len(taskInfos) == 0 {
		return nil, err
	}
	log.Infof("model task:%+v", taskInfos[0])
	clusterInfo, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil {
		return nil, err
	}
	var isObserver bool
	if input.Operation != def.OPERATION_SUPERVISOR_START {
		if !nodeInfo.IsWitness {
			role, err := redis_op.GetNodeRole(address, input.ClusterId, input.GroupId, clusterInfo.Name)
			if err != nil {
				return nil, err
			}

			if role == def.NODE_ROLE_MASTER {
				return nil, errors.New("master node")
			}

			if input.UpdateConfig != "false" && role == def.NODE_ROLE_SINGLE {
				return nil, errors.New("could not updateConfig single node")
			}

			if role == def.NODE_ROLE_OBSERVER {
				isObserver = true
			}
			if role == def.NODE_ROLE_SLAVE || role == def.NODE_ROLE_MASTER {
				if err = dashboard.SetDashboardCookie(ctx); err != nil {
					return nil, err
				}
				err = dashboard.ReplicaNode(ctx, address, clusterInfo.Name, 0, input.GroupId)
				if err != nil {
					return nil, err
				}
				err = dashboard.SyncGroup(ctx, clusterInfo.Name, input.GroupId)
				if err != nil {
					return nil, err
				}
			}
		}
		if !tbl_group.LockGroup(input.ClusterId, input.GroupId, true) {
			return nil, errors.New("locked by others.Please unlock and retry")
		}
	}
	initRaft, groupInfo, err := mdl_group.GetGroupInfo(nodeInfo.ClusterId, nodeInfo.GroupId, nodeInfo.ServiceId)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		log.Warnf("get group info clusterId:%d groupId:%d failed.err:%v", nodeInfo.ClusterId, nodeInfo.GroupId, err)
		return nil, err
	}
	var extString string
	if input.Operation == def.OPERATION_SUPERVISOR_START {
		extString = mdl_resource_pool.FormatMachineCgroup(nodeInfo.ServicePort, clusterInfo.Id, machineInfo.ID, machineInfo.IDC)
	}
	task := &tbl_task.Task{
		Type:           def.TASK_TYPE_UPGRADE,
		Status:         def.TASK_NEW,
		RegionId:       nodeInfo.RegionId,
		MachineId:      machineInfo.ID,
		ServiceId:      nodeInfo.ServiceId,
		ClusterId:      input.ClusterId,
		GroupId:        input.GroupId,
		NodeId:         input.NodeId,
		CosFileId:      input.CosFileId,
		CosFileVersion: input.Version,
		TaskExt: tbl_task.TaskExtra{
			Ip:               machineInfo.IP,
			RegionName:       taskInfos[0].TaskExt.RegionName,
			ServiceName:      serviceInfo.Name,
			HostName:         machineInfo.HostName,
			ServicePort:      nodeInfo.ServicePort,
			ClusterPort:      nodeInfo.ClusterPort,
			ClusterName:      clusterInfo.Name,
			DashboardName:    taskInfos[0].TaskExt.DashboardName,
			DashboardAddress: config.GetConf().Domains.Dashboard,
			CloudType:        machineInfo.IDC,
			NodeIndex:        int(input.NodeId),
			NodeList:         initRaft,
			NodeIdList:       groupInfo.InitNodeId,
			NodeListVal:      fmt.Sprintf("%s:%d", machineInfo.IP, nodeInfo.ClusterPort),
			NodeListStr:      groupInfo.InitRaft,
			Operation:        input.Operation,
			IsWitness:        nodeInfo.IsWitness,
			IsObserver:       isObserver,
			DeraftToken:      math2.GetMd5(clusterInfo.Name),
			StoredAuth:       "",
			UpdateConfig:     input.UpdateConfig != "false",
			ExtString:        extString,
		},
	}
	err = tbl_task.CreateTask(task)
	if err != nil {
		log.Warn("create task failed.err:", err)
		return nil, err
	}
	return nil, nil
}

func multiUpgradeServer(ctx *gin.Context, clusterInfo *tbl_cluster.Cluster, groupNode *GroupNode, input *MultiUpgradeNodeInput, replicas map[string]bool, upgradeMaster bool) (map[uint]string, error) {
	nodeIds := groupNode.NodeIds
	groupId := groupNode.GroupId
	operation := input.Operation
	cosFileId := input.CosFileId
	updateConfig := input.UpdateConfig
	masterNodeCloud := make(map[uint]string)

	ids := strings.Split(nodeIds, ",")
	clusterId := clusterInfo.Id

	taskInfos, err := tbl_task.GetClusterTask(clusterId)
	if err != nil || len(taskInfos) == 0 {
		log.Warnf("clusterId:%d groupId:%d  get init task fail err:%v", clusterId, groupId, err)
		return nil, err
	}
	taskInfo := taskInfos[0]
	regionName := taskInfo.TaskExt.RegionName
	if len(regionName) <= 0 {
		regionInfo, err := tbl_region.GetInfo(taskInfo.RegionId)
		if err != nil {
			return nil, err
		}
		regionName = regionInfo.Name
	}
	dashboardName := taskInfo.TaskExt.DashboardName
	log.Infof("clusterId:%d groupId:%d task:%+v", clusterId, groupId, taskInfo)

	for _, strNodeId := range ids {
		nodesId, _ := strconv.ParseUint(strNodeId, 10, 64)
		nodeId := uint(nodesId)
		nodeInfo, err := tbl_node.GetInfo(nodeId, groupId, clusterId)
		if err != nil {
			return masterNodeCloud, err
		}
		if nodeInfo.Status == def.NODE_STATUS_OFFLINE {
			log.Warnf("clusterId:%d groupId:%d nodeId:%d upgrading an offline node", clusterId, groupId, nodeId)
			continue
		}

		if !def.ServciceIdIsServer(nodeInfo.ServiceId) {
			log.Warnf("clusterId:%d groupId:%d nodeId:%d service id not match. service_id:%d", clusterId, groupId, nodeId, nodeInfo.ServiceId)
			continue
		}

		canUpgrade := toolkit.CheckVersion(nodeInfo.CosFileVersion, input.CosFileVersion)
		if !canUpgrade {
			log.Warnf("multigrade can't upgrade this version, old=%s new=%s", nodeInfo.CosFileVersion, input.CosFileVersion)
			continue
		}
		machineInfo, err := tbl_machine.GetInfo(nodeInfo.MachineId)
		if err != nil {
			log.Warnf("clusterId:%d groupId:%d nodeId:%d get machine info fail err:%v", clusterId, groupId, nodeId, err)
			continue
		}
		address := machineInfo.IP + ":" + strconv.Itoa(int(nodeInfo.ServicePort))
		replica := replicas[address]

		serviceInfo, err := tbl_service.GetInfo(nodeInfo.ServiceId)
		if err != nil {
			log.Warnf("clusterId:%d groupId:%d nodeId:%d get service info fail err:%v", clusterId, groupId, nodeId, err)
			continue
		}

		initRaft, groupInfo, err := mdl_group.GetGroupInfo(nodeInfo.ClusterId, nodeInfo.GroupId, nodeInfo.ServiceId)
		if err != nil {
			log.Warnf("clusterId:%d groupId:%d nodeId:%d get group fail err=%v", clusterId, groupId, nodeId, err)
			continue
		}

		var isObserver bool
		if operation == def.OPERATION_BITALOS_UPGRADE {
			role, err := redis_op.GetNodeRole(address, clusterId, groupId, clusterInfo.Name)
			if role == def.NODE_ROLE_MASTER || err != nil {
				log.Warnf("clusterId:%d groupId:%d nodeId:%d could not shutdown master node", clusterId, groupId, nodeId)
				masterNodeCloud[nodeId] = machineInfo.IDC
				continue
			}
			if role == def.NODE_ROLE_OBSERVER {
				isObserver = true
			}

			log.Infof("multigrade groupId:%d start lock", groupId)
			if !tbl_group.LockGroup(clusterId, groupId, true) {
				hasLockGroup := false
				for i := 0; i < 6; i++ {
					time.Sleep(time.Second * 10)
					if !tbl_group.LockGroup(clusterId, groupId, true) {
						log.Warnf("%d time try to lock group", i)
						continue
					}
					hasLockGroup = true
					break
				}
				if !hasLockGroup {
					log.Warnf("groupId:%d locked by others", groupId)
					continue
				}
			}
		}
		task := &tbl_task.Task{
			Type:           def.TASK_TYPE_UPGRADE,
			Status:         def.TASK_NEW,
			RegionId:       nodeInfo.RegionId,
			MachineId:      machineInfo.ID,
			ServiceId:      nodeInfo.ServiceId,
			ClusterId:      clusterId,
			GroupId:        groupId,
			NodeId:         nodeId,
			CosFileId:      cosFileId,
			CosFileVersion: input.CosFileVersion,
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
				NodeIndex:        int(nodeId),
				NodeList:         initRaft,
				NodeIdList:       groupInfo.InitNodeId,
				NodeListStr:      groupInfo.InitRaft,
				NodeListVal:      fmt.Sprintf("%s:%d", machineInfo.IP, nodeInfo.ClusterPort),
				Operation:        operation,
				IsWitness:        nodeInfo.IsWitness,
				IsObserver:       isObserver,
				DeraftToken:      math2.GetMd5(clusterInfo.Name),
				StoredAuth:       "",
				UpdateConfig:     updateConfig != "false",
			},
		}
		if replica {
			log.Infof("multigrade clusterId:%d groupId:%d nodeId:%d start offline replica", clusterId, groupId, nodeId)
			if err := dashboard.SetDashboardCookie(ctx); err != nil {
				log.Warnf("clusterId:%d groupId:%d nodeId:%d set dashboard cookie failed.err=%v", clusterId, groupId, nodeId, err)
				continue
			}
			err := dashboard.ReplicaNode(ctx, address, clusterInfo.Name, 0, groupNode.GroupId)
			if err != nil {
				log.Warnf("clusterId:%d groupId:%d nodeId:%d offline replica fail err:%v", clusterId, groupId, nodeId, err)
				continue
			}
			log.Infof("multigrade clusterId:%d groupId:%d nodeId:%d start sync group", clusterId, groupId, nodeId)
			err = dashboard.SyncGroup(ctx, clusterInfo.Name, groupNode.GroupId)
			if err != nil {
				log.Warnf("clusterId:%d groupId:%d nodeId:%d sync fail err:%v", clusterId, groupId, nodeId, err)
				continue
			}
		}
		err = tbl_task.CreateTask(task)
		if err != nil {
			tbl_group.LockGroup(clusterId, groupId, false)
			log.Warnf("clusterId:%d groupId:%d nodeId:%d create task failed.err=%v", clusterId, groupId, nodeId, err)
			continue
		}
		mdl_ops.CreateOpsActionLog(nodeInfo, task)

		if replica {
			for i := 0; i < 18; i++ {
				time.Sleep(time.Second * 10)
				log.Infof("multigrade clusterId:%d groupId:%d nodeId:%d %d time check group lock", clusterId, groupId, nodeId, i)
				if !tbl_group.GetGroupLock(clusterId, groupId) {
					_, err := redis_cli.NewClient(address, config.GetAuth(clusterId, ""), 5*time.Second)
					if err != nil {
						log.Errorf("could not connect to redis.err:%+v", err)
						continue
					}
					log.Infof("multigrade clusterId:%d groupId:%d nodeId:%d start online replica", clusterId, groupId, nodeId)
					err = dashboard.ReplicaNode(ctx, address, clusterInfo.Name, 1, groupNode.GroupId)
					if err != nil {
						log.Warnf("clusterId:%d groupId:%d nodeId:%d online replica fail err:%v", clusterId, groupId, nodeId, err)
						continue
					}
					log.Infof("multigrade clusterId:%d groupId:%d nodeId:%d has online err:%v", clusterId, groupId, nodeId, err)
					break
				}
			}
		}
	}

	return masterNodeCloud, nil
}

func upgradeMaster(ctx *gin.Context, clusterInfo *tbl_cluster.Cluster, groupId uint, masterNodeId, slaveNodeId uint, input *MultiUpgradeNodeInput, replicas map[string]bool) error {
	if slaveNodeId <= 0 {
		log.Warnf("clusterId:%d groupId:%d nodeId:%d slave node empty", clusterInfo.Id, groupId, slaveNodeId)
		return nil
	}
	slaveNodeInfo, err := tbl_node.GetInfo(slaveNodeId, groupId, clusterInfo.Id)
	if err != nil {
		return err
	}
	slaveMachineInfo, err := tbl_machine.GetInfo(slaveNodeInfo.MachineId)
	if err != nil {
		log.Warnf("clusterId:%d groupId:%d nodeId:%d get master machine info fail err:%v", clusterInfo.Id, groupId, slaveNodeId, err)
	}
	slaveAddress := slaveMachineInfo.IP + ":" + strconv.Itoa(int(slaveNodeInfo.ServicePort))

	if err := dashboard.SetDashboardCookie(ctx); err != nil {
		log.Warnf("groupId:%d set dashboard cookie failed.err=%v", groupId, err)
		return err
	}
	log.Infof("multigrade groupId:%d start promote master to nodeId: %d", groupId, slaveNodeId)
	err = dashboard.Promote(ctx, clusterInfo.Name, slaveAddress, groupId)
	if err != nil {
		log.Errorf("multigrade groupId:%d promote failed err:%v", groupId, err)
		return err
	}
	time.Sleep(time.Second * 8)
	strMasterNodeId := strconv.FormatUint(uint64(masterNodeId), 10)
	groupNode := &GroupNode{GroupId: groupId, NodeIds: strMasterNodeId}
	_, err = multiUpgradeServer(ctx, clusterInfo, groupNode, input, replicas, true)
	if err != nil {
		return err
	}
	return nil
}

func GetReplica(dashboardName string) (map[string]bool, error) {
	topom, err := dashboard.GetTopom(dashboardName)
	if err != nil {
		log.Warnf("failed to get dashboard topom.address:%s err:%+v", config.GetConf().Domains.DashboardDomain, err)
		return nil, err
	}
	replicaMap := make(map[string]bool, 0)
	if topom != nil {
		for _, f := range topom.Data.Stats.Group.Models {
			for _, s := range f.Servers {
				replicaMap[s.Addr] = s.ReplicaGroup
			}
		}
	}
	return replicaMap, nil
}

func multiStopNode(clusterInfo *tbl_cluster.Cluster, groupNode *GroupNode, input *MultiUpgradeNodeInput, replicas map[string]bool, replicaGroupIds map[uint]map[string]string) error {
	nodeIds := groupNode.NodeIds
	groupId := groupNode.GroupId
	operation := input.Operation
	cosFileId := input.CosFileId
	updateConfig := input.UpdateConfig
	ids := strings.Split(nodeIds, ",")
	clusterId := clusterInfo.Id

	taskInfos, err := tbl_task.GetClusterTask(clusterId)
	if err != nil || len(taskInfos) == 0 {
		log.Warnf("clusterId:%d groupId:%d  get init task fail err:%v", clusterId, groupId, err)
		return err
	}
	taskInfo := taskInfos[0]
	regionName := taskInfo.TaskExt.RegionName
	if len(regionName) <= 0 {
		regionInfo, err := tbl_region.GetInfo(taskInfo.RegionId)
		if err != nil {
			return err
		}
		regionName = regionInfo.Name
	}
	dashboardName := taskInfo.TaskExt.DashboardName
	log.Infof("clusterId:%d groupId:%d task:%+v", clusterId, groupId, taskInfo)

	for _, strNodeId := range ids {
		nodesId, _ := strconv.ParseUint(strNodeId, 10, 64)
		nodeId := uint(nodesId)
		nodeInfo, err := tbl_node.GetInfo(nodeId, groupId, clusterId)
		if err != nil {
			return err
		}
		if nodeInfo.Status == def.NODE_STATUS_OFFLINE {
			log.Warnf("clusterId:%d groupId:%d nodeId:%d is an offline node, continue", clusterId, groupId, nodeId)
			continue
		}
		machineInfo, err := tbl_machine.GetInfo(nodeInfo.MachineId)
		if err != nil {
			log.Warnf("clusterId:%d groupId:%d nodeId:%d get machine info fail err:%v", clusterId, groupId, nodeId, err)
			continue
		}
		address := machineInfo.IP + ":" + strconv.Itoa(int(nodeInfo.ServicePort))
		replica := replicas[address]
		if replica {
			log.Warnf("clusterId:%d groupId:%d nodeId:%d has replica, continue", clusterId, groupId, nodeId)
			continue
		}
		if _, ok := replicaGroupIds[groupId]; ok {
			infos, err := redis_op.GetInfo(address, input.ClusterId, groupId, clusterInfo.Name)
			if err != nil {
				log.Warnf("get info fail, err:%v", err.Error())
				continue
			}
			role, ok := infos["role"]
			if ok && role == "master" {
				log.Warnf("could not shutdown master node")
				continue
			}
			if clusterStatus, ok := infos["status"]; ok {
				if clusterStatus == "true" {
					log.Warnf("clusterId:%d groupId:%d nodeId:%d cluster status is true, continue", clusterId, groupId, nodeId)
					continue
				}
			}
		}

		serviceInfo, err := tbl_service.GetInfo(nodeInfo.ServiceId)
		if err != nil {
			log.Warnf("clusterId:%d groupId:%d nodeId:%d get service info fail err:%v", clusterId, groupId, nodeId, err)
			continue
		}

		initRaft, groupInfo, err := mdl_group.GetGroupInfo(nodeInfo.ClusterId, nodeInfo.GroupId, nodeInfo.ServiceId)
		if err != nil {
			log.Warnf("clusterId:%d groupId:%d nodeId:%d get group fail err=%v", clusterId, groupId, nodeId, err)
			continue
		}
		task := &tbl_task.Task{
			Type:      def.TASK_TYPE_OPERATE,
			Status:    def.TASK_NEW,
			RegionId:  nodeInfo.RegionId,
			MachineId: machineInfo.ID,
			ServiceId: nodeInfo.ServiceId,
			ClusterId: clusterId,
			GroupId:   groupId,
			NodeId:    nodeId,
			CosFileId: cosFileId,
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
				NodeIndex:        int(nodeId),
				NodeList:         initRaft,
				NodeIdList:       groupInfo.InitNodeId,
				NodeListStr:      groupInfo.InitRaft,
				NodeListVal:      fmt.Sprintf("%s:%d", machineInfo.IP, nodeInfo.ClusterPort),
				Operation:        operation,
				IsWitness:        nodeInfo.IsWitness,
				IsObserver:       false,
				DeraftToken:      math2.GetMd5(clusterInfo.Name),
				StoredAuth:       "",
				UpdateConfig:     updateConfig != "false",
			},
		}
		err = tbl_task.CreateTask(task)
		if err != nil {
			tbl_group.LockGroup(clusterId, groupId, false)
			log.Warnf("clusterId:%d groupId:%d nodeId:%d create task failed.err=%v", clusterId, groupId, nodeId, err)
			continue
		}
	}
	return nil
}
