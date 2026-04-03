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

package mdl_node

import (
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_config"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cosfile"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_group"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_regionmachine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/dao/web/dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_group"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_port"
	"github.com/zuoyebang/bitalostored/paas/model/redis_op"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func RemoveOneWitness(ctx *gin.Context, clusterId uint, idc string) (interface{}, error) {
	clusterInfo, err := tbl_cluster.GetInfo(clusterId)
	if err != nil {
		log.Warn("get cluster info failed.err:", err)
		return nil, err
	}
	if clusterInfo.ServiceId != def.SERVICE_ID_BITALOS && clusterInfo.ServiceId != def.SERVICE_ID_MATRIX {
		return nil, errors.New("service is not bitalos")
	}
	dashboardName, err := mdl_dashboard.GetDashboardName(clusterInfo.StoredId)
	if err != nil {
		return nil, err
	}
	regionId := clusterInfo.RegionId
	regionInfo, err := tbl_region.GetNewRegion(regionId)
	if err != nil {
		return nil, err
	}

	witnessList, err := tbl_node.GetWitnessListByCluster(clusterId)
	if err != nil {
		return nil, err
	}
	if err := dashboard.SetDashboardCookie(ctx); err != nil {
		log.Warnf("cluster:%s set dashboard cookie failed.err=%v", clusterInfo.Name, err)
		return nil, err
	}
	removedGroups := make(map[uint]struct{}, 10)
	for _, nodeInfo := range witnessList {
		if nodeInfo.Status == def.NODE_STATUS_NEW || !nodeInfo.IsWitness {
			continue
		}
		machineId := nodeInfo.MachineId
		machineInfo, err := tbl_machine.GetInfo(machineId)
		if err != nil {
			log.Warnf("get mdl_machine failed.err:%+v", err)
			continue
		}
		if machineInfo.IDC != idc {
			continue
		}
		groupId := nodeInfo.GroupId
		if _, ok := removedGroups[groupId]; ok {
			continue
		} else {
			removedGroups[groupId] = struct{}{}
		}
		go func(nodeInfo *tbl_node.Node) {
			log.Infof("The node is to be removed. cluster:%s group:%d ip:%s port:%d", clusterInfo.Name, groupId, machineInfo.IP, nodeInfo.ServicePort)
			if _, err := RemoveWitness(ctx, groupId, nodeInfo, clusterInfo, dashboardName, regionInfo, machineInfo); err != nil {
				log.Errorf("remove witness err:%s cluster:%s group:%d node:%d ip:%s port:%d", err, clusterInfo.Name, groupId, nodeInfo.NodeId, machineInfo.IP, nodeInfo.ServicePort)
			}
		}(nodeInfo)
	}
	return len(removedGroups), nil
}

// Func: Create all witness nodes for the cluster
func CreateAllWitness(clusterId, cosFileId uint, idc string) (interface{}, error) {
	clusterInfo, err := tbl_cluster.GetInfo(clusterId)
	if err != nil {
		log.Warn("get cluster info failed.err:", err)
		return nil, err
	}
	if !def.IsServer(clusterInfo.ServiceId) {
		return nil, errors.New("service is not bitalos")
	}
	dashboardName, err := mdl_dashboard.GetDashboardName(clusterInfo.StoredId)
	if err != nil {
		return nil, err
	}
	regionId := clusterInfo.RegionId
	newRegion, err := tbl_region.GetNewRegion(regionId)
	if err != nil {
		return nil, err
	}
	cosFile, err := tbl_cosfile.GetCosFile(cosFileId)
	if err != nil {
		return nil, err
	}
	_ = cosFile
	groups, err := tbl_group.GetList(clusterId, "", -1, 0)
	if err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		return nil, nil
	}

	machineCount := GetWitnessCountByRegion(newRegion.ID, idc)
	machineNum := len(machineCount)
	machineIdx := 0
	getNextMachine := func(clusterId, groupId uint) uint {
		for i := 0; i < 8; i++ {
			machineId := machineCount[machineIdx%machineNum].MachineId
			exists, err := tbl_node.CheckMachineDuplicatedByGroup(clusterId, groupId, machineId)
			if err != nil {
				return 0
			}
			machineIdx++
			if !exists {
				return machineId
			}
		}
		return 0
	}
	for _, groupInfo := range groups {
		groupId := groupInfo.GroupId
		machineId := getNextMachine(clusterId, groupId)
		if machineId == 0 {
			log.Errorf("create witness error:[machine not found] cluster:%s group:%d", clusterInfo.Name, groupId)
			continue
		}
		if _, err := CreateWitness(machineId, groupId, cosFile, clusterInfo, dashboardName, newRegion); err != nil {
			log.Errorf("create witness error:%s cluster:%s group:%d", err, clusterInfo.Name, groupId)
		}
	}
	return nil, nil
}

type MachineWitnessCount struct {
	MachineId uint
	Count     int
}

func GetWitnessCountByRegion(regionId uint, idc string) []MachineWitnessCount {
	machineList := make([]*tbl_machine.Machine, 0)
	if regionId > 0 {
		machineIds, err := tbl_regionmachine.GetMachinesByRegion(regionId)
		if err != nil {
			return nil
		}
		machineList, err = tbl_machine.GetOnlineListByIds(machineIds)
		if err != nil {
			return nil
		}
	} else {
		om, err := tbl_machine.GetOnlineMachines()
		if err != nil {
			return nil
		}
		machineList = om
	}

	newMachineIds := make([]uint, 0, 10)
	for _, machineInfo := range machineList {
		if machineInfo.IDC == idc && machineInfo.IsVirtual == def.MACHINE_NOT_VIRTUAL {
			newMachineIds = append(newMachineIds, machineInfo.ID)
		}
	}

	counts := make([]MachineWitnessCount, 0, len(newMachineIds))
	for _, mid := range newMachineIds {
		if total, err := tbl_node.CountMachineNode(mid, def.SERVICE_ID_BITALOS, true); err == nil {
			counts = append(counts, MachineWitnessCount{
				MachineId: mid,
				Count:     total,
			})
		}
	}

	sort.Slice(counts, func(i, j int) bool {
		return counts[i].Count < counts[j].Count
	})

	return counts
}

// Func: Create a witness task for the group
// Input: regionId clusterId machineId groupId cosFileId
// Output: create result
func CreateWitness(machineId, groupId uint, cosFile *tbl_cosfile.CosFile, clusterInfo *tbl_cluster.Cluster, dashboardName string, regionInfo *tbl_region.Region) (interface{}, error) {
	clusterId := clusterInfo.Id
	serviceId := uint(def.SERVICE_ID_BITALOS)
	regionId := regionInfo.ID
	regionName := regionInfo.Name

	serviceInfo, err := tbl_service.GetInfo(serviceId)
	if err != nil {
		log.Warnf("get service info failed.serviceId:%d", serviceId)
		return nil, err
	}

	machine, err := tbl_machine.GetInfo(machineId)
	if err != nil {
		log.Warnf("get mdl_machine failed.err:%+v", err)
		return nil, err
	}

	initRaft, groupInfo, err := mdl_group.GetGroupInfo(clusterId, groupId, serviceId)
	if err != nil {
		log.Warn("get group info failed.err:", err)
		return nil, err
	}

	taskType := def.TASK_TYPE_PREPAREADD
	nodeInfo := &tbl_node.Node{
		ClusterId:      clusterId,
		GroupId:        groupId,
		CosFileId:      cosFile.ID,
		CosFileVersion: cosFile.Version,
		RegionId:       regionId,
		MachineId:      machineId,
		ServiceId:      serviceId,
		IsWitness:      true,
	}
	pod, err := tbl_node.Create(nodeInfo, groupInfo.MaxNodeId)
	if err != nil {
		return nil, err
	}
	e := tbl_group.Update(clusterId, groupId, tbl_group.Group{
		MaxNodeId: pod.NodeId,
	})
	if e != nil {
		return nil, err
	}
	task := &tbl_task.Task{
		Type:           taskType,
		Status:         def.TASK_NEW,
		RegionId:       regionId,
		MachineId:      machine.ID,
		ServiceId:      serviceId,
		ClusterId:      clusterId,
		GroupId:        groupId,
		NodeId:         pod.NodeId,
		CosFileId:      cosFile.ID,
		CosFileVersion: cosFile.Version,
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
			IsWitness:        true,
			IsObserver:       false,
			NodeList:         initRaft,
			NodeIdList:       groupInfo.InitNodeId,
			NodeListStr:      groupInfo.InitRaft,
			//NodeListVal:      fmt.Sprintf("%s:%d", machine.IP, nodeInfo.ClusterPort),
			NodeIndex:    int(pod.NodeId),
			DeraftToken:  math2.GetMd5(clusterInfo.Name),
			UpdateConfig: true,
		},
	}
	err = tbl_task.CreateTask(task)
	if err != nil {
		log.Warn("create task info failed. err:", err)
	}
	log.Infof("add task:%+v", task)
	return nil, err
}

func RemoveWitness(ctx *gin.Context, groupId uint, nodeInfo *tbl_node.Node, clusterInfo *tbl_cluster.Cluster, dashboardName string, regionInfo *tbl_region.Region, machineInfo *tbl_machine.Machine) (interface{}, error) {
	regionName := regionInfo.Name
	clusterName := clusterInfo.Name
	serverAddr := machineInfo.IP + ":" + strconv.Itoa(int(nodeInfo.ServicePort))
	nodeIsDown := false

	if !redis_op.IsOnline(serverAddr, nodeInfo.ClusterId) {
		nodeIsDown = true
	} else {
		// node is up
		checkNodeStatus := func() bool {
			status, _ := redis_op.CheckRaftStatus(serverAddr, clusterInfo.Id, nodeInfo.GroupId)
			return status
		}
		nodeStatus := checkNodeStatus()
		if nodeStatus {
			// Call dashboard api to remove it
			// The witness node is belong to one raft cluster
			nodeRaftAddr := machineInfo.IP + ":" + strconv.Itoa(int(nodeInfo.ClusterPort))
			if err := dashboard.RemoveServerNode(ctx, clusterInfo.Name, int(nodeInfo.GroupId), serverAddr, nodeRaftAddr, int(nodeInfo.NodeId)); err != nil {
				return nil, err
			}
			// check node status until it becomes false
			for i := 0; i <= 10; i++ {
				nodeStatus = checkNodeStatus()
				if !nodeStatus {
					break
				}
				time.Sleep(6 * time.Second)
			}
		}
		if nodeStatus {
			log.Infof("node is live, remove it manually. cluster:%s node:%s", clusterInfo.Name, serverAddr)
			return nil, errors.New("can not remove the node, which status is true")
		}

		_, err := CloseWitness(clusterInfo, nodeInfo, machineInfo, regionName, dashboardName)
		if err != nil {
			return nil, err
		}
		// Check node is not connected, which means the node is down.
		for i := 0; i < 10; i++ {
			if !redis_op.IsOnline(serverAddr, nodeInfo.ClusterId) {
				nodeIsDown = true
				break
			}
			time.Sleep(3 * time.Second)
		}
	}
	if nodeIsDown {
		// Purpose: Remove it from dashboard
		if err := dashboard.DelServerNode(ctx, clusterName, groupId, serverAddr, int(nodeInfo.NodeId)); err != nil {
			log.Errorf("delete server false. cluster:%s node:%s error:%s", clusterName, serverAddr, err)
		}
	}
	return nil, nil
}

func CloseWitness(clusterInfo *tbl_cluster.Cluster, n *tbl_node.Node, machineInfo *tbl_machine.Machine, regionName, dashboardName string) (interface{}, error) {
	clusterName := clusterInfo.Name
	if clusterName == "our-search-page" {
		clusterName = "ocr-search-page"
	} else if clusterName == "our-search-inv" {
		clusterName = "ocr-search-inv"
	}
	nodeOperation := "supervisor-stop"
	serviceName := def.SERVICE_BITALOS
	taskType := def.TASK_TYPE_OPERATE
	task := &tbl_task.Task{
		Type:      taskType,
		Status:    def.TASK_NEW,
		RegionId:  n.RegionId,
		MachineId: n.MachineId,
		ServiceId: n.ServiceId,
		ClusterId: n.ClusterId,
		GroupId:   n.GroupId,
		NodeId:    n.NodeId,
		CosFileId: n.CosFileId,
		TaskExt: tbl_task.TaskExtra{
			Ip:               machineInfo.IP,
			RegionName:       regionName,
			ServiceName:      serviceName,
			HostName:         machineInfo.HostName,
			ServicePort:      n.ServicePort,
			ClusterPort:      n.ClusterPort,
			ClusterName:      clusterName,
			DashboardName:    dashboardName,
			DashboardAddress: config.GetConf().Domains.Dashboard,
			CloudType:        machineInfo.IDC,
			NodeIndex:        int(n.NodeId),
			Operation:        nodeOperation,
			IsWitness:        true,
			IsObserver:       false,
			DeraftToken:      "",
			StoredAuth:       "",
			UpdateConfig:     false,
		},
	}
	log.Infof("add stop task. task:%+v", *task)
	err := tbl_task.CreateTask(task)
	if err != nil {
		log.Warn("create task failed.err:", err)
		return nil, err
	}
	return nil, nil
}

func GetListByMachine(machineId uint, limit int, offset int) ([]*tbl_node.Node, error) {
	return tbl_node.GetListByMachine(machineId, limit, offset)
}

func CopyNode(srcNode *tbl_node.Node, groupId uint) (*tbl_node.Node, error) {
	return tbl_node.Copy(srcNode, groupId)
}

func SetNodeStatus(taskId uint, status string) error {
	taskInfo, err := tbl_task.GetInfo(taskId)
	if err != nil {
		log.Warnf("get task info failed.taskId:%d", taskId)
		return err
	}
	if taskInfo.Type == def.TASK_TYPE_CGROUP {
		return nil
	}

	cid := taskInfo.ClusterId
	gid := taskInfo.GroupId
	nid := taskInfo.NodeId
	nodeDetail, err := tbl_node.GetInfo(nid, gid, cid)
	if err != nil {
		log.Infof("get node info failed. err:%+v", err)
		return err
	}
	if nodeDetail.Status != def.NODE_STATUS_NEW {
		return nil
	}
	nodeStatus := def.NODE_STATUS_ONLINE
	if status == def.TASK_FAIL {
		nodeStatus = def.NODE_STATUS_OFFLINE
	}
	log.Infof("node status update. nodeId:%d,groupId:%d,clusterId:%d,updateStatus:%s", nid, gid, cid, nodeStatus)
	err = tbl_node.Update(nid, gid, cid, tbl_node.Node{Status: nodeStatus})
	if err != nil {
		log.Warn("update node status failed")
		return err
	}
	return nil
}

func NotifyDashboard(ctx *gin.Context, taskId uint) error {
	taskInfo, err := tbl_task.GetInfo(taskId)
	if err != nil {
		log.Warnf("get task info failed.taskId:%d", taskId)
		return err
	}
	if !tbl_task.CheckNotifyType(taskInfo.Type) {
		log.Info("not need to notify dashboard.It's not a create operation")
		return err
	}
	if !def.IsServer(taskInfo.ServiceId) {
		log.Info("not need to notify dashboard.It's not a server")
		return err
	}
	if dashboard.SetDashboardCookie(ctx) != nil {
		log.Warn("set dashboard request cookie failed.")
		return errors.New("set dashboard request cookie failed")
	}

	if taskInfo.TaskExt.IsWitness || taskInfo.TaskExt.IsObserver {
		nodeType := "observer_node"
		if taskInfo.TaskExt.IsWitness {
			nodeType = "witness_node"
		}
		machineInfo, err := tbl_machine.GetInfo(taskInfo.MachineId)
		if err != nil {
			return err
		}
		time.Sleep(time.Second * 10)
		address := fmt.Sprintf("%s:%d", machineInfo.IP, taskInfo.TaskExt.ServicePort)
		err = dashboard.AddNodeToGroup(ctx, dashboard.MachineInfo{
			IpPort: address,
			IDC:    machineInfo.IDC,
		}, taskInfo.TaskExt.DashboardName, int(taskInfo.GroupId), nodeType)
		if err != nil {
			log.Warnf("add node to dashboard failed.Will try again after 20 seconds.err:%v", err)
			time.Sleep(time.Second * 20)
			err = dashboard.AddNodeToGroup(ctx, dashboard.MachineInfo{
				IpPort: address,
				IDC:    machineInfo.IDC,
			}, taskInfo.TaskExt.DashboardName, int(taskInfo.GroupId), nodeType)
			if err != nil {
				log.Warnf("failed to add node to dashboard.err:%v", err)
				return err
			}
		}
		return nil
	}

	nodeList, err := tbl_node.GetNodesByStatus(def.NODE_STATUS_ONLINE, taskInfo.ClusterId, taskInfo.GroupId)
	if err != nil {
		log.Warnf("get node info failed.err:%+v.nodeId:%d", err, taskInfo.NodeId)
	}
	if len(taskInfo.TaskExt.NodeList) != len(nodeList) {
		log.Infof("not ready for notifying dashboard. task-nodeList:%v node-nodeList-num:%d", taskInfo.TaskExt.NodeList, len(nodeList))
		return nil
	}
	err = dashboard.CreateNewGroup(ctx, taskInfo.TaskExt.DashboardName, int(taskInfo.GroupId))
	if err != nil {
		log.Errorf("create new group failed in dashboard.err:%+v.groupId:%d", err, int(taskInfo.GroupId))
		return err
	}
	var machineInfos []dashboard.MachineInfo
	for _, nodeInfo := range nodeList {
		machineInfo, err := tbl_machine.GetInfo(nodeInfo.MachineId)
		if err != nil {
			log.Warnf("get machine info failed.err:%+v.nodeInfo:%+v", err, nodeInfo)
			return err
		}
		machineInfos = append(machineInfos, dashboard.MachineInfo{
			IpPort: machineInfo.IP + ":" + strconv.Itoa(int(nodeInfo.ServicePort)),
			IDC:    machineInfo.IDC,
		})
	}
	err = dashboard.AddNodesToGroup(ctx, machineInfos, taskInfo.TaskExt.DashboardName, int(taskInfo.GroupId))
	if err != nil {
		log.Warnf("failed to add node to dashboard.err:%v", err)
		time.Sleep(time.Second * 20)
		err = dashboard.AddNodesToGroup(ctx, machineInfos, taskInfo.TaskExt.DashboardName, int(taskInfo.GroupId))
		if err != nil {
			log.Warnf("failed to add node to dashboard.err:%v", err)
			return err
		}
	}

	return nil
}

func Prepared(nodeId, groupId, clusterId uint, servicePort, clusterPort uint) error {
	return tbl_node.Update(nodeId, groupId, clusterId, tbl_node.Node{
		ClusterPort: clusterPort,
		ServicePort: servicePort,
	})
}

func UpdateNodeConfig(configPackId uint, task *tbl_task.Task) error {
	configs, err := tbl_config.GetListByPack(configPackId, task.ServiceId)
	if err != nil {
		return err
	}
	var content string
	for _, conf := range configs {
		if conf.Name == "config/config.toml" {
			content = conf.Content
		}
	}
	if len(content) <= 0 {
		return errors.New("no config")
	}
	newContent, err := config.Render(content, task)
	if err != nil {
		log.Warn("render file failed.err:", err)
		return err
	}
	err = tbl_node.Update(task.NodeId, task.GroupId, task.ClusterId, tbl_node.Node{ConfigContent: newContent})
	if err != nil {
		log.Warn("update node failed.err:", err)
		return err
	}
	return nil
}

func UpdateNodeConfigPort(configPackId uint, task *tbl_task.Task, newPort uint) (string, error) {
	configs, err := tbl_config.GetListByPack(configPackId, task.ServiceId)
	if err != nil {
		return "", err
	}
	var content string
	for _, conf := range configs {
		if conf.Name == "config/config.toml" {
			content = conf.Content
		}
	}
	if len(content) <= 0 {
		return "", errors.New("no config")
	}
	newContent, err := config.Render(content, task)
	if err != nil {
		log.Warn("render file failed.err:", err)
		return "", err
	}
	updateTime := time.Now().Unix()
	sql := fmt.Sprintf("update %s set config_content = '%s', cluster_port = %d, update_time='%d' where node_id = %d and group_id = %d and cluster_id = %d",
		tbl_node.TableName, newContent, newPort, updateTime, task.NodeId, task.GroupId, task.ClusterId)
	return sql, err
}

func GetNodeIdByAddress(nodeList []string, groupId, clusterId uint) ([]int, error) {
	var ips []string
	var nodeIds []int
	for _, nodePort := range nodeList {
		exp := strings.Split(nodePort, ":")
		if len(exp) != 2 {
			log.Warnf("nodeport not port, %s", nodePort)
			continue
		}
		ips = append(ips, exp[0])
	}
	machines, err := tbl_machine.GetMachinesByIpList(ips)
	if err != nil {
		return nodeIds, err
	}
	machineIds := make(map[string]uint)
	for _, machine := range machines {
		machineIds[machine.IP] = machine.ID
	}
	i := 1
	var initNodeIds []int
	for _, nodePort := range nodeList {
		exp := strings.Split(nodePort, ":")
		if len(exp) != 2 {
			log.Warnf("nodeport not port, %s", nodePort)
			continue
		}
		if machineId, ok := machineIds[exp[0]]; ok {
			port, _ := strconv.ParseUint(exp[1], 10, 64)
			nodeList, err := tbl_node.GetNodeByClusterPort(clusterId, groupId, uint(port), machineId)
			if err != nil {
				log.Warnf("get node id failed, ip=%s, clusterport=%d", exp[0], port)
				continue
			}
			for _, node := range nodeList {
				nodeIds = append(nodeIds, int(node.NodeId))
			}
			initNodeIds = append(initNodeIds, i)
		} else {
			initNodeIds = append(initNodeIds, i)
		}
		i++
	}
	if len(nodeIds) != len(nodeList) {
		nodeIds = initNodeIds
	}
	return nodeIds, nil
}
