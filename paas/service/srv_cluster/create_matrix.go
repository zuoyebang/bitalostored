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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_cluster"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_port"
	"github.com/zuoyebang/bitalostored/paas/model/strategy"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"strings"

	"github.com/gin-gonic/gin"
)

type CreateMatrixInput struct {
	RegionId      uint   `json:"regionId"`
	MachineIdList []uint `json:"machineIdList"`
	ServiceId     uint   `json:"serviceId"`
	CosFileId     uint   `json:"packageId"`
	ConfigPackId  uint   `json:"configPackId"`
	ClusterName   string `json:"clusterName"`
	GroupSum      uint   `json:"groupSum"`
	NodeSum       uint   `json:"nodeSum"`
	StoredId      uint   `json:"storedId"`
	StoredAuth    string `json:"storedAuth"`

	Strategy    string `json:"strategy"`
	PriorityIDC string `json:"priorityIDC"`
	Operation   string `json:"operation"`
	IpList      string `json:"ipList"`
	Ips         []string
}

var _ servicer.Servicer = new(CreateMatrixInput)

func (input *CreateMatrixInput) CheckParams(ctx *gin.Context) error {
	if input.GroupSum <= 0 {
		return errors.New("invalid groupSum")
	}
	if input.RegionId <= 0 {
		return errors.New("invalid regionId")
	}
	if input.NodeSum <= 0 {
		return errors.New("invalid nodeSum")
	}
	if input.NodeSum >= 8 || input.GroupSum >= 25 {
		return errors.New("nodeSum or groupSum too large")
	}
	if input.CosFileId <= 0 {
		return errors.New("invalid cosFileId")
	}
	if input.StoredId <= 0 {
		return errors.New("invalid storedId")
	}
	if input.ServiceId <= 0 {
		return errors.New("invalid serviceId")
	}
	if input.ConfigPackId <= 1 {
		return errors.New("invalid configPackId. Do not use the default config")
	}
	if strings.TrimSpace(input.Strategy) == "" {
		log.Warnf("not assign strategy.using default strategy")
		input.Strategy = def.MACHINEBALANCE
	}
	input.ClusterName = strings.TrimSpace(input.ClusterName)
	if input.ClusterName <= "" {
		return errors.New("invalid clusterName")
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

func (input *CreateMatrixInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	taskNum := 0
	input.ServiceId = def.SERVICE_ID_BITALOS
	var err error
	machineMapList, err := strategy.FormatMachines(input.RegionId, input.ServiceId, 0, input.Ips, false)
	if err != nil {
		log.Warn("get format machine list failed.err:", err)
		return nil, err
	}
	if len(machineMapList) == 0 {
		return CreateClusterOutput{ClusterId: 0, TaskCreated: 0}, errors.New("there is no machine")
	}
	log.Infof("machine map list:%+v.", machineMapList)
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

	groupMachines, err := strategy.SplitMachinesByGroup(input.PriorityIDC, int(input.GroupSum), int(input.NodeSum), machineMapList)
	if err != nil {
		log.Warn("get group machine list failed.err:", err)
		return nil, err
	}
	//log.Info("group machines distribution.", groupMachines)

	serviceInfo, regionInfo, clusterInfo, err := mdl_cluster.GetBasicInfo(input.ServiceId, input.RegionId, input.StoredId, input.ConfigPackId, input.ClusterName, "")
	if err != nil {
		log.Warn("get basic info failed.err:", err)
		return nil, err
	}

	idcs := []string{def.IdcTxcloud, def.IdcAli}
	for _, idc := range idcs {
		if _, e := tbl_resource_pool.Create(input.ClusterName, def.CGROUP_NAME_CPU, idc, clusterInfo.Id, input.ServiceId, 0, def.DEFAULT_CGROUP_CPU); e != nil {
			log.Warnf("create resource pool error %v. cluster:%s", e, input.ClusterName)
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
	}

	return CreateClusterOutput{ClusterId: clusterInfo.Id, TaskCreated: taskNum}, err
}
