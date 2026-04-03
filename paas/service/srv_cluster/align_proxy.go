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
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cosfile"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_group"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_hostport"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/dao/web/dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_machine"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_ops"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/model/redis_op"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strconv"
	"strings"
	"time"
)

type AlignProxyInput struct {
	ClusterId uint   `json:"clusterId"`
	CosFileId uint   `json:"packageId"`
	Operation string `json:"operation"`
	IpList    string `json:"ipList"`
	Ips       []string
}

var _ servicer.Servicer = new(AlignProxyInput)

func (input *AlignProxyInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	if input.CosFileId <= 0 {
		return errors.New("invalid version")
	}
	if len(input.IpList) <= 0 {
		return errors.New("invalid ip")
	}
	ips := strings.Split(input.IpList, "\n")
	if len(ips) <= 0 {
		return errors.New("invalid ip")
	}
	input.Ips = ips
	return nil
}

func (input *AlignProxyInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	if input.Operation == def.OPERATION_SUPERVISOR_START {
		return input.SupervisorStartProxy()
	} else if input.Operation == def.OPERATION_SUPERVISOR_STOP {
		return input.SupervisorStopProxy(ctx)
	}
	return nil, nil
}

func (input *AlignProxyInput) SupervisorStartProxy() (interface{}, error) {
	taskNum := 0
	clusterInfo, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil || clusterInfo == nil {
		log.Warnf("get cluster info failed.err:%+v", err)
		return taskNum, err
	}
	regionId := clusterInfo.RegionId
	regionName := clusterInfo.Name
	regionInfo, err := tbl_region.GetInfo(clusterInfo.RegionId)
	if err != nil || regionInfo == nil {
		log.Warnf("get region info failed.err:%v", err)
		return taskNum, err
	}
	if regionInfo.NewId > 0 {
		regionId = regionInfo.NewId
	}
	serviceInfo, err := tbl_service.GetInfo(clusterInfo.ServiceId)
	if err != nil || serviceInfo == nil {
		log.Warnf("get region info failed.err:%v", err)
		return taskNum, err
	}
	cosFile, err := tbl_cosfile.GetCosFile(input.CosFileId)
	if err != nil {
		return nil, err
	}
	installedNode, err := tbl_node.GetOneByClusterId(clusterInfo.Id)
	if err != nil {
		return []*tbl_machine.Machine{}, err
	}
	if installedNode.NodeId <= 0 {
		return []*tbl_machine.Machine{}, errors.New("should install proxy first")
	}
	machineIpInfos, err := mdl_machine.GetMachinesByIps(input.Ips)
	if err != nil {
		return nil, err
	}

	for _, machine := range machineIpInfos {
		if _, err := tbl_hostport.Create(machine.ID, installedNode.ServicePort, machine.IP); err != nil {
			isAlive := redis_op.IsOnline(machine.IP+":"+strconv.Itoa(int(installedNode.ServicePort)), input.ClusterId)
			if isAlive {
				log.Warnf("ip:%s assign host port:%d failed. err:%v", machine.IP, installedNode.ServicePort, err)
				continue
			}
		}

		if _, err := tbl_hostport.Create(machine.ID, installedNode.ClusterPort, machine.IP); err != nil {
			isAlive := redis_op.IsOnline(machine.IP+":"+strconv.Itoa(int(installedNode.ClusterPort)), input.ClusterId)
			if isAlive {
				log.Warnf("ip:%s assign host port:%d failed. err:%v", machine.IP, installedNode.ClusterPort, err)
				continue
			}
		}
		nodeInfo := &tbl_node.Node{
			ClusterId:      clusterInfo.Id,
			GroupId:        1,
			CosFileId:      cosFile.ID,
			CosFileVersion: cosFile.Version,
			RegionId:       regionId,
			MachineId:      machine.ID,
			ServiceId:      clusterInfo.ServiceId,
			ServicePort:    installedNode.ServicePort,
			ClusterPort:    installedNode.ClusterPort,
		}
		node, err := tbl_node.Create(nodeInfo, 0)
		if err != nil {
			return nil, err
		}
		e := tbl_group.Update(clusterInfo.Id, 1, tbl_group.Group{
			MaxNodeId: node.NodeId,
		})
		if e != nil {
			log.Errorf("update max_node_id fail.gid:%d maxNodeId:%d err:%v", 1, node.NodeId, e)
		}
		extString := mdl_resource_pool.FormatMachineCgroup(installedNode.ServicePort, clusterInfo.Id, machine.ID, machine.IDC)
		task := &tbl_task.Task{
			Type:           def.TASK_TYPE_START,
			Status:         def.TASK_NEW,
			RegionId:       regionId,
			MachineId:      machine.ID,
			ServiceId:      clusterInfo.ServiceId,
			ClusterId:      clusterInfo.Id,
			GroupId:        1,
			NodeId:         node.NodeId,
			CosFileId:      input.CosFileId,
			CosFileVersion: cosFile.Version,
			TaskExt: tbl_task.TaskExtra{
				Ip:               machine.IP,
				RegionName:       regionName,
				ServiceName:      serviceInfo.Name,
				ServicePort:      installedNode.ServicePort,
				ClusterPort:      installedNode.ClusterPort,
				ClusterName:      clusterInfo.Name,
				CloudType:        machine.IDC,
				Operation:        input.Operation,
				StoredAuth:       "",
				UpdateConfig:     true,
				DashboardAddress: config.GetConf().Domains.Dashboard,
				ExtString:        extString,
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
		mdl_ops.CreateOpsActionLog(node, task)
		taskNum++
		log.Infof("add task:%+v", task)
	}

	return taskNum, err
}

func (input *AlignProxyInput) SupervisorStopProxy(ctx *gin.Context) (interface{}, error) {
	taskNum := 0

	clusterInfo, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil || clusterInfo == nil {
		log.Warnf("get cluster info failed.err:%+v", err)
		return taskNum, err
	}
	if clusterInfo.ServiceId != def.SERVICE_ID_PROXY {
		return taskNum, errors.New("not support non-proxy cluster")
	}

	regionId := clusterInfo.RegionId
	regionName := clusterInfo.Name
	regionInfo, err := tbl_region.GetInfo(clusterInfo.RegionId)
	if err != nil || regionInfo == nil {
		log.Warnf("get region info failed.err:%v", err)
		return taskNum, err
	}
	if regionInfo.NewId > 0 {
		regionId = regionInfo.NewId
	}

	serviceInfo, err := tbl_service.GetInfo(clusterInfo.ServiceId)
	if err != nil || serviceInfo == nil {
		log.Warnf("get region info failed.err:%v", err)
		return taskNum, err
	}

	proxyServicePort, proxyAdminPort, err := getProxyPort(clusterInfo.Id)
	if err != nil {
		return taskNum, err
	}

	machineIpInfos, err := mdl_machine.GetMachinesByIps(input.Ips)
	if err != nil {
		return taskNum, err
	}

	dashboard.SetDashboardCookie(ctx)
	clusterName := clusterInfo.Name

	for _, machine := range machineIpInfos {
		nodeInfo, err := tbl_node.GetProxyNodeByMachine(input.ClusterId, machine.ID)
		if err != nil {
			log.Warnf("get node info error. machine:%d cluster:%d err:%v", machine.ID, input.ClusterId, err)
			continue
		}
		task := &tbl_task.Task{
			Type:      def.TASK_TYPE_OPERATE,
			Status:    def.TASK_NEW,
			RegionId:  regionId,
			MachineId: machine.ID,
			ServiceId: def.SERVICE_ID_PROXY,
			ClusterId: clusterInfo.Id,
			GroupId:   1,
			NodeId:    nodeInfo.NodeId,
			CosFileId: input.CosFileId,
			TaskExt: tbl_task.TaskExtra{
				Ip:           machine.IP,
				RegionName:   regionName,
				ServiceName:  serviceInfo.Name,
				ServicePort:  proxyServicePort,
				ClusterPort:  proxyAdminPort,
				ClusterName:  clusterInfo.Name,
				CloudType:    machine.IDC,
				Operation:    input.Operation,
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
			nodeAddr := machine.IP + ":" + strconv.Itoa(int(proxyServicePort))
			go func(hostname, hostport string) {
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
					}
				} else {
					log.Infof("check proxy online hostport:%s", hostport)
				}
			}(machine.HostName, nodeAddr)
		}
		taskNum++
		log.Infof("add task:%+v", task)
	}

	return taskNum, err
}

func getProxyPort(clusterId uint) (uint, uint, error) {
	installedNode, err := tbl_node.GetOneByClusterId(clusterId)
	if err != nil {
		return 0, 0, err
	}
	if installedNode.NodeId <= 0 {
		return 0, 0, errors.New("should install proxy first")
	}
	return installedNode.ServicePort, installedNode.ClusterPort, nil
}

func filterMachines(clusterId, regionId, nodeNum uint, idc string, machineIdList []uint) ([]*tbl_machine.Machine, *tbl_node.Node) {
	var machineList []*tbl_machine.Machine
	var err error
	chooseMachine := false
	if len(machineIdList) > 0 {
		machineList, err = tbl_machine.GetList(machineIdList)
		if err != nil {
			return []*tbl_machine.Machine{}, &tbl_node.Node{}
		}
		chooseMachine = true
	}
	installedNodes, err := tbl_node.GetListByCluster(clusterId)
	if err != nil {
		log.Warnf("get proxy nodes failed.err:%+v", err)
		return []*tbl_machine.Machine{}, &tbl_node.Node{}
	}
	if len(installedNodes) == 0 {
		log.Warnf("should install proxy first.")
		return []*tbl_machine.Machine{}, &tbl_node.Node{}
	}
	for _, node := range installedNodes {
		for index, m := range machineList {
			if m.ID == node.MachineId {
				machineList = append(machineList[:index], machineList[index+1:]...)
				break
			}
		}
	}
	log.Infof("uninstall node machine num:%d", len(machineList))
	if !chooseMachine {
		machineList = copyMachineLice(machineList, int(nodeNum))
	}
	log.Infof("choose machine num:%d", len(machineList))
	return machineList, installedNodes[0]
}

func copyMachineLice(machineList []*tbl_machine.Machine, num int) []*tbl_machine.Machine {
	if num > len(machineList) {
		return machineList
	}
	newMachineList := make([]*tbl_machine.Machine, num)
	copy(newMachineList, machineList[:num])
	return newMachineList
}
