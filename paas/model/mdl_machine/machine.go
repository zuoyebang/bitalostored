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

package mdl_machine

import (
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_regionmachine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/dao/web/dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/redis_op"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func Offline(machineId uint) error {
	return tbl_machine.Update(machineId, tbl_machine.Machine{Status: def.MACHINE_STATUS_OFFLINE})
}

func MultiOffline(machineIds []uint) error {
	return tbl_machine.MultiOfflineMachine(machineIds)
}

func GetMachinesByRegion(regionId uint, status []string) []*tbl_machine.Machine {
	if regionId <= 0 {
		machineList, _ := tbl_machine.GetOnlineMachines()
		return machineList
	}
	machineIdList, err := tbl_regionmachine.GetMachinesByRegion(regionId)
	if err != nil {
		log.Warnf("failed to get region machines.err:%+v", err)
		return nil
	}
	machineList, err := tbl_machine.GetList(machineIdList)
	if err != nil {
		log.Warnf("failed to get region machines.err:%+v", err)
		return nil
	}
	if len(status) == 0 {
		return machineList
	}
	var m []*tbl_machine.Machine
	for _, machine := range machineList {
		// nodes, _ := tbl_node.GetMachineOnlineNodes(machine.ID, 1, false)
		// machineList[i].IP = machineList[i].IP + "_" + strconv.Itoa(len(nodes))
		for _, s := range status {
			if s == machine.Status {
				m = append(m, machine)
			}
		}
	}
	return m
}

func GetClusterNameByMachineId(clusterList []*tbl_cluster.Cluster, mid uint, serviceId uint, isWitness bool) []string {
	nodeList, err := tbl_node.GetMachineOnlineNodes(mid, serviceId, isWitness)
	if err != nil {
		return nil
	}
	clusterIds := make(map[uint]int, 10)
	for _, n := range nodeList {
		if _, ok := clusterIds[n.ClusterId]; !ok {
			clusterIds[n.ClusterId] = 1
		}
	}
	res := make([]string, 0, 10)
	for _, c := range clusterList {
		if _, ok := clusterIds[c.Id]; ok {
			res = append(res, c.Name)
		}
	}
	return res
}

func GetMachinesByIps(ips []string) (map[string]*tbl_machine.Machine, error) {
	machines, err := tbl_machine.GetMachinesByIpList(ips)
	if err != nil {
		return nil, err
	}
	machineIds := make(map[string]*tbl_machine.Machine, len(machines))
	for _, machine := range machines {
		machineIds[machine.IP] = machine
	}
	return machineIds, nil
}

func StopAndRemoveProxy(ctx *gin.Context, machineInfo *tbl_machine.Machine, nodeInfo *tbl_node.Node) error {
	clusterInfo, err := tbl_cluster.GetInfo(nodeInfo.ClusterId)
	if err != nil {
		return err
	}
	regionInfo, err := tbl_region.GetInfo(clusterInfo.RegionId)
	if err != nil {
		return err
	}
	dashboard.SetDashboardCookie(ctx)
	action := "supervisor-stop"
	task := &tbl_task.Task{
		Type:      def.TASK_TYPE_OPERATE,
		Status:    def.TASK_NEW,
		RegionId:  clusterInfo.RegionId,
		MachineId: machineInfo.ID,
		ServiceId: def.SERVICE_ID_PROXY,
		ClusterId: clusterInfo.Id,
		GroupId:   1,
		NodeId:    nodeInfo.NodeId,
		CosFileId: nodeInfo.CosFileId,
		TaskExt: tbl_task.TaskExtra{
			Ip:           machineInfo.IP,
			RegionName:   regionInfo.Name,
			ServiceName:  def.SERVICE_STORED_PROXY,
			ServicePort:  nodeInfo.ServicePort,
			ClusterPort:  nodeInfo.ClusterPort,
			ClusterName:  clusterInfo.Name,
			CloudType:    machineInfo.IDC,
			Operation:    action,
			StoredAuth:   "",
			UpdateConfig: false,
		},
	}
	err = tbl_task.CreateTask(task)
	if err != nil {
		log.Warn("create task info failed.err:", err)
		err = tbl_task.CreateTask(task)
		if err != nil {
			log.Warnf("create task info failed again.err:%v", err)
		}
	}
	if err == nil {
		// After proxy is stopped, remove it from dashboard
		nodeAddr := machineInfo.IP + ":" + strconv.Itoa(int(nodeInfo.ServicePort))
		go func(hostname, clusterName, hostport string) {
			nodeIsDown := false
			// Check node is not connected, which means the node is down.
			for i := 0; i < 10; i++ {
				if !redis_op.IsOnline(hostport, nodeInfo.ClusterId) {
					nodeIsDown = true
					break
				}
				time.Sleep(3 * time.Second)
			}
			if nodeIsDown {
				if err := dashboard.RemoveProxyNode(ctx, clusterName, hostname, hostport); err != nil {
					log.Infof("remove proxy err:%s hostport:%s", err, hostport)
				} else {
					log.Infof("remove proxy succ cluster:%s hostport:%s", clusterName, hostport)
				}
				if err := tbl_node.Update(nodeInfo.NodeId, nodeInfo.GroupId, nodeInfo.ClusterId, tbl_node.Node{Status: def.NODE_STATUS_OFFLINE}); err != nil {
					log.Infof("offline proxy err:%s hostport:%s", err, hostport)
				} else {
					log.Infof("offline proxy succ cluster:%s hostport:%s", clusterName, hostport)
				}
			} else {
				log.Infof("check proxy online hostport:%s", hostport)
			}
		}(machineInfo.HostName, clusterInfo.Name, nodeAddr)
	}
	return nil
}
