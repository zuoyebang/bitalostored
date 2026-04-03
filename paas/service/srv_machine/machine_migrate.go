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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cosfile"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_group"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/dao/web/dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_task"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strconv"

	"github.com/gin-gonic/gin"
)

type MachineMigrateInput struct {
	SourceIp string `json:"sourceIp"`
	TargetIp string `json:"targetIp"`
}

var _ servicer.Servicer = new(MachineMigrateInput)

func (input *MachineMigrateInput) CheckParams(ctx *gin.Context) error {
	if len(input.SourceIp) <= 0 || len(input.TargetIp) <= 0 {
		return errors.New("invalid ip")
	}
	if input.SourceIp == input.TargetIp {
		return errors.New("source IP and target IP cannot be the same")
	}
	return nil
}

func (input *MachineMigrateInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	sourceInfo, err := tbl_machine.GetMachineInfo(input.SourceIp)
	if err != nil {
		return nil, err
	}
	if sourceInfo.ID <= 0 {
		return nil, errors.New("invalid source ip")
	}
	targetInfo, err := tbl_machine.GetMachineInfo(input.TargetIp)
	if err != nil {
		return nil, err
	}
	if targetInfo.ID <= 0 {
		return nil, errors.New("invalid target ip")
	}
	if sourceInfo.IsVirtual != targetInfo.IsVirtual {
		return nil, errors.New("The source machine and the target machine must both be virtual machines or physical machines.")
	}

	sourceNodeList, err := tbl_node.GetOnlineNodes(sourceInfo.ID)
	if err != nil {
		log.Warn("get machine nodelist failed.err:", err)
		return nil, err
	}

	serviceList, err := tbl_service.GetList(-1, 0)
	if err != nil {
		log.Warn("get service  list failed.err:", err)
		return nil, err
	}
	serviceMap := make(map[uint]*tbl_service.Service, len(serviceList))
	for i := 0; i < len(serviceList); i++ {
		serviceMap[serviceList[i].ID] = serviceList[i]
	}

	results := make([]string, 0)
	for _, sourceNodeInfo := range sourceNodeList {
		sourceNode := fmt.Sprintf("%s:%d", sourceInfo.IP, sourceNodeInfo.ServicePort)
		clusterInfo, err := tbl_cluster.GetInfo(sourceNodeInfo.ClusterId)
		if err != nil {
			res := formatResult("unknow cluster", sourceNodeInfo.ServiceId, sourceNode, "get cluster info failed", err)
			results = append(results, res)
			continue
		}
		targetNode := fmt.Sprintf("%s:%d", targetInfo.IP, sourceNodeInfo.ServicePort)
		regionInfo, err := tbl_region.GetInfo(sourceNodeInfo.RegionId)
		if err != nil {
			res := formatResult(clusterInfo.Name, sourceNodeInfo.ServiceId, targetNode, "get region info failed", err)
			results = append(results, res)
			continue
		}
		regionName := regionInfo.Name
		cosFileId := sourceNodeInfo.CosFileId
		cosFileVersion := sourceNodeInfo.CosFileVersion
		if def.IsServer(sourceNodeInfo.ServiceId) {
			cf, err := tbl_cosfile.GetMaxVersion(def.SERVICE_ID_BITALOS)
			if err != nil {
				res := formatResult(clusterInfo.Name, sourceNodeInfo.ServiceId, targetNode, "get cos file failed", err)
				results = append(results, res)
				continue
			}
			if cf == nil {
				res := formatResult(clusterInfo.Name, sourceNodeInfo.ServiceId, targetNode, "get cos file id failed", nil)
				results = append(results, res)
				continue
			}
			cosFileId = cf.ID
			cosFileVersion = cf.Version
		}
		groupInfo, err := tbl_group.GetInfo(sourceNodeInfo.ClusterId, sourceNodeInfo.GroupId)
		if err != nil {
			res := formatResult(clusterInfo.Name, sourceNodeInfo.ServiceId, targetNode, "get group info failed", err)
			results = append(results, res)
			continue
		}
		nodeInfo := &tbl_node.Node{
			ClusterId:      sourceNodeInfo.ClusterId,
			GroupId:        sourceNodeInfo.GroupId,
			CosFileId:      cosFileId,
			CosFileVersion: cosFileVersion,
			RegionId:       sourceNodeInfo.RegionId,
			MachineId:      targetInfo.ID,
			ServiceId:      sourceNodeInfo.ServiceId,
			IsWitness:      sourceNodeInfo.IsWitness,
		}
		maxNodeId := groupInfo.MaxNodeId
		if sourceNodeInfo.ServiceId == def.SERVICE_ID_PROXY {
			maxNodeId = 0
		}
		pod, err := tbl_node.Create(nodeInfo, maxNodeId)
		if err != nil {
			res := formatResult(clusterInfo.Name, sourceNodeInfo.ServiceId, targetNode, "insert tbl_node failed", err)
			results = append(results, res)
			continue
		}
		e := tbl_group.Update(clusterInfo.Id, sourceNodeInfo.GroupId, tbl_group.Group{
			MaxNodeId: pod.NodeId,
		})
		if e != nil {
			log.Errorf("update max_node_id fail.gid:%d maxNodeId:%d err:%v", sourceNodeInfo.GroupId, pod.NodeId, e)
		}
		task := &tbl_task.Task{
			RegionId:  sourceNodeInfo.RegionId,
			MachineId: targetInfo.ID,
			ServiceId: sourceNodeInfo.ServiceId,
			ClusterId: sourceNodeInfo.ClusterId,
			GroupId:   sourceNodeInfo.GroupId,
			NodeId:    pod.NodeId,
			CosFileId: cosFileId,
			TaskExt: tbl_task.TaskExtra{
				Ip:           targetInfo.IP,
				RegionName:   regionName,
				ServicePort:  sourceNodeInfo.ServicePort,
				ClusterPort:  sourceNodeInfo.ClusterPort,
				ServiceName:  def.GetServiceNameFromServiceId(int(sourceNodeInfo.ServiceId)),
				ClusterName:  clusterInfo.Name,
				CloudType:    targetInfo.IDC,
				UpdateConfig: true,
				HostName:     targetInfo.HostName,
			},
		}
		task, msg, err := mdl_task.FormatCreateNodeTask(task, sourceNodeInfo.IsWitness, serviceMap[task.ServiceId], clusterInfo)
		if len(msg) > 0 {
			res := formatResult(clusterInfo.Name, sourceNodeInfo.ServiceId, targetNode, msg, err)
			results = append(results, res)
			continue
		}

		err = tbl_task.CreateTask(task)
		if err != nil {
			res := formatResult(clusterInfo.Name, sourceNodeInfo.ServiceId, targetNode, "create task failed", err)
			results = append(results, res)
			continue
		}
		res := formatResult(clusterInfo.Name, sourceNodeInfo.ServiceId, targetNode, "create task success", err)
		results = append(results, res)
		log.Infof("node operation task:%+v", task)
	}
	return results, nil
}

func formatResult(clusterName string, serviceId uint, address, msg string, err error) string {
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	return fmt.Sprintf("[%s]-[%s]-[%s] %s %s", clusterName, def.GetServiceNameFromServiceId(int(serviceId)), address, msg, errStr)
}

func getMasterVersionCosFile(clusterName string, clusterId, groupId uint, sourceNode string) uint {
	dh, err := dashboard.GetTopom(clusterName)
	if err != nil {
		log.Errorf("%s gettopom failed, err %v", clusterName, err)
		return 0
	}
	anotherNode := ""
	if dh.Data.Stats.Group.Models != nil {
		for _, group := range dh.Data.Stats.Group.Models {
			if group.Id == groupId {
				for _, server := range group.Servers {
					if server.Addr != sourceNode && server.Role == "master_slave_node" {
						anotherNode = server.Addr
						break
					}
				}
				break
			}
		}
	}

	if len(anotherNode) > 0 {
		leaderNode := ""
		leaderNodeId := 0
		for node, stats := range dh.Data.Stats.Group.Stats {
			if node == anotherNode {
				if la, ok := stats.Stats["leader_address"]; ok {
					leaderNode = la
					leaderNodeId, _ = strconv.Atoi(stats.Stats["leader_node_id"])
				}
			}
		}
		if len(leaderNode) > 0 {
			if leaderNode == sourceNode {
				return 0
			}
			nodeInfo, err := tbl_node.GetInfo(uint(leaderNodeId), groupId, clusterId)
			if err != nil {
				return 0
			}
			return nodeInfo.CosFileId
		}
	}
	return 0
}
