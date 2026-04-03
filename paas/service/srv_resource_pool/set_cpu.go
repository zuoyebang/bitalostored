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

package srv_resource_pool

import (
	"github.com/gin-gonic/gin"
	jsoniter "github.com/json-iterator/go"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type CpuInput struct {
	Id         int `json:"id"`
	CpuSetType int `json:"cpuSetType"`
}

func (input *CpuInput) CheckParams(ctx *gin.Context) error {
	if input.Id <= 0 {
		return errors.New("empty id")
	}
	return nil
}

func (input *CpuInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	err := tbl_resource_pool.UpdateCpuSet(input.Id, input.CpuSetType)
	if err != nil {
		return nil, err
	}
	resourceList, err := tbl_resource_pool.GetResourceByIds([]int{input.Id})
	if err != nil {
		return nil, err
	}
	if len(resourceList) <= 0 {
		return nil, errors.New("invalid id")
	}
	if input.CpuSetType == def.NOT_SET_CPU {
		nodes, err := tbl_node.GetOnlineClusterMachine(resourceList[0].ClusterId, resourceList[0].ServiceId, false)
		if err != nil {
			log.Errorf("apply failed clusterId:%d serviceId:%d err:%+v", resourceList[0].ClusterId, resourceList[0].ServiceId, err)
			return nil, err
		}
		machineIds := make([]uint, 0)
		for _, node := range nodes {
			machineIds = append(machineIds, node.MachineId)
		}
		machineInfos, err := tbl_machine.GetList(machineIds)
		if err != nil {
			log.Errorf("get machineInfos:%v faild err:%v", machineIds, err)
		}
		delMids := make(map[uint]int, 0)
		if len(machineInfos) > 0 {
			for _, machineInfo := range machineInfos {
				if machineInfo.IDC != resourceList[0].IDC {
					delMids[machineInfo.ID] = 1
				}
			}
		}

		formatV := make(map[uint][]uint, 0)
		for _, node := range nodes {
			if _, ok := delMids[node.MachineId]; ok {
				continue
			}
			if formatV[node.MachineId] == nil {
				formatV[node.MachineId] = make([]uint, 0)
			}
			formatV[node.MachineId] = append(formatV[node.MachineId], node.ServicePort)
		}
		for mid, ports := range formatV {
			err = mdl_resource_pool.ChangeCpuSetType(mid, ports, def.NOT_SET_CPU)
			if err != nil {
				log.Warnf("mid:%d ports:%v release cpus faild", mid, ports)
			}
			ext, _ := jsoniter.MarshalToString(ports)
			task := &tbl_task.Task{
				Type:      def.TASK_TYPE_CGROUP,
				Status:    def.TASK_NEW,
				MachineId: mid,
			}
			task.TaskExt.ExtString = ext
			task.TaskExt.Operation = def.OPERATION_CGROUP_RELEASE_CPUS
			err := tbl_task.CreateTask(task)
			if err != nil {
				log.Errorf("create task faild machineId:%d err:%+v", mid, err)
			}
		}
	}
	if input.CpuSetType == def.EXCLUSIVE_CPU {
		mdl_resource_pool.GenerateCgroupTask(resourceList, true, false, true, true)
	}
	if input.CpuSetType == def.SHARE_CPU {
		mdl_resource_pool.GeneralShareCpuTask(resourceList)
	}
	return nil, err
}
