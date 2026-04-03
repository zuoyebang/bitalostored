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

package srv_group

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cosfile"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_group"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_port"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"strings"
)

type CreateGroupsInput struct {
	ClusterId   uint   `json:"clusterId"`
	GroupSum    uint   `json:"groupSum"`
	NodeSum     uint   `json:"nodeSum"`
	Strategy    string `json:"strategy"`
	CosFileId   uint   `json:"packageId"`
	PriorityIDC string `json:"priorityIDC"`
	Operation   string `json:"operation"`
	IpList      string `json:"ipList"`
	Ips         []string
}

var _ servicer.Servicer = new(CreateGroupsInput)

func (input *CreateGroupsInput) CheckParams(ctx *gin.Context) error {
	if input.NodeSum <= 0 {
		return errors.New("invalid nodeSum")
	}
	if input.GroupSum != 1 {
		return errors.New("invalid groupSum(must=1)")
	}
	if input.NodeSum >= 12 {
		return errors.New("nodeSum too large")
	}
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	if input.CosFileId <= 0 {
		return errors.New("invalid cosFileId")
	}
	if strings.TrimSpace(input.Strategy) == "" {
		log.Warnf("not assign strategy.using default strategy")
		input.Strategy = def.MACHINEBALANCE
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

func (input *CreateGroupsInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	taskNum := 0
	cosFileInfo, err := tbl_cosfile.GetCosFile(input.CosFileId)
	if err != nil {
		return nil, err
	}
	if cosFileInfo.ID <= 0 {
		return nil, errors.New("can't find cos file")
	}
	clusterInfo, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil {
		log.Warn("get basic info failed.err:", err)
		return nil, err
	}
	regionInfo, err := tbl_region.GetInfo(clusterInfo.RegionId)
	if err != nil {
		log.Warn("get region info failed.err:", err)
		return nil, err
	}

	clusterInfo.ServiceId = def.SERVICE_ID_BITALOS
	regionId := clusterInfo.RegionId
	regionName := regionInfo.Name
	if regionInfo.NewId > 0 {
		regionId = regionInfo.NewId
		newInfo, err := tbl_region.GetInfo(regionInfo.NewId)
		if err != nil {
			return nil, err
		}
		regionName = newInfo.Name
	}
	serviceInfo, err := tbl_service.GetInfo(clusterInfo.ServiceId)
	if err != nil {
		log.Warn("get basic info failed.err:", err)
		return nil, err
	}
	machineList, err := tbl_machine.GetMachinesByIpList(input.Ips)
	if err != nil || len(machineList) == 0 {
		log.Warn("get machine list failed.err:", err, " machines:", len(machineList))
		return nil, err
	}
	createNodeNum := math2.MinInt(len(machineList), int(input.NodeSum))
	machineList = machineList[0:createNodeNum]

	dashboardName, err := mdl_dashboard.GetDashboardName(clusterInfo.StoredId)
	if err != nil {
		return nil, err
	}

	grp, err := tbl_group.Create(clusterInfo.Id, clusterInfo.ServiceId)
	if err != nil {
		log.Warn("create group failed.err:", err)
		return nil, err
	}

	var maxNodeId uint
	for _, m := range machineList {
		nodeInfo := &tbl_node.Node{
			ClusterId:      clusterInfo.Id,
			GroupId:        grp.GroupId,
			CosFileId:      input.CosFileId,
			CosFileVersion: cosFileInfo.Version,
			RegionId:       regionId,
			MachineId:      m.ID,
			ServiceId:      clusterInfo.ServiceId,
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
			RegionId:       regionId,
			MachineId:      m.ID,
			ServiceId:      clusterInfo.ServiceId,
			ClusterId:      clusterInfo.Id,
			GroupId:        grp.GroupId,
			NodeId:         pod.NodeId,
			CosFileId:      input.CosFileId,
			CosFileVersion: cosFileInfo.Version,
			TaskExt: tbl_task.TaskExtra{
				Ip:               m.IP,
				RegionName:       regionName,
				ServiceName:      serviceInfo.Name,
				HostName:         m.HostName,
				ServicePortRange: mdl_port.NarrowDownPortRange(serviceInfo.PortRanges, m.ID),
				ClusterPortRange: mdl_port.NarrowDownPortRange(serviceInfo.ClusterPortRanges, m.ID),
				ClusterName:      clusterInfo.Name,
				DashboardName:    dashboardName,
				NodeIndex:        int(pod.NodeId),
				CloudType:        m.IDC,
				Operation:        input.Operation,
				DeraftToken:      math2.GetMd5(clusterInfo.Name),
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
	if maxNodeId > 0 {
		e := tbl_group.Update(clusterInfo.Id, grp.GroupId, tbl_group.Group{
			MaxNodeId: maxNodeId,
		})
		if e != nil {
			log.Errorf("update max_node_id fail.gid:%d maxNodeId:%d err:%v", grp.GroupId, maxNodeId, e)
		}
	}
	return taskNum, err
}
