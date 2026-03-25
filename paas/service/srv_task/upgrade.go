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

package srv_task

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cosfile"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_group"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_task"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strconv"
)

type TaskUpgradedInput struct {
	TaskId uint   `json:"taskId"`
	Status string `json:"taskStatus"`
}

var _ servicer.Servicer = new(TaskUpgradedInput)

func (input *TaskUpgradedInput) CheckParams(ctx *gin.Context) error {
	return nil
}
func (input *TaskUpgradedInput) BuildOutput(ctx *gin.Context) (interface{}, error) {

	if err := mdl_task.UpdateTaskStatus(input.TaskId, input.Status); err != nil {
		return nil, err
	}
	if input.Status == def.TASK_FAIL {
		return nil, nil
	}
	taskInfo, err := tbl_task.GetInfo(input.TaskId)
	if err != nil {
		log.Warn("get task info failed.err:", err)
		return nil, err
	}
	cosFile, _ := tbl_cosfile.GetCosFile(taskInfo.CosFileId)
	version := ""
	if cosFile != nil {
		version = cosFile.Version
	}
	err = tbl_node.Update(taskInfo.NodeId, taskInfo.GroupId, taskInfo.ClusterId, tbl_node.Node{CosFileId: taskInfo.CosFileId, CosFileVersion: version})
	if err != nil {
		log.Warn("update node version failed.err:", err)
	}

	if taskInfo.TaskExt.Operation == def.OPERATION_SUPERVISOR_STOP {
		err = mdl_resource_pool.ChangeCpuSetType(taskInfo.MachineId, []uint{taskInfo.TaskExt.ServicePort}, def.NOT_SET_CPU)
		if err != nil {
			log.Errorf("taskId:%d release cpus faild err:%v", taskInfo.ID, err)
			return nil, err
		}
	}

	if taskInfo.TaskExt.ServiceName != def.SERVICE_MATRIX && taskInfo.TaskExt.ServiceName != def.SERVICE_BITALOS {
		return nil, nil
	}

	address := taskInfo.TaskExt.Ip + ":" + strconv.Itoa(int(taskInfo.TaskExt.ServicePort))

	go mdl_group.RoutineUpgradeRedis(address, taskInfo.TaskExt.ClusterName, taskInfo.ClusterId, taskInfo.GroupId)
	return nil, nil
}
