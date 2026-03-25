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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_task"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"

	"github.com/gin-gonic/gin"
)

type TasksStatus struct {
	TaskIds []uint `json:"taskIds"`
	Status  string `json:"taskStatus"`
}

var _ servicer.Servicer = new(TasksStatus)

func (input *TasksStatus) CheckParams(ctx *gin.Context) error {
	if input.Status == "" {
		return errors.New("invalid taskStatus")
	}
	return nil
}
func (input *TasksStatus) BuildOutput(ctx *gin.Context) (interface{}, error) {
	switch input.Status {
	case def.TASK_SUCCESS:
		for _, taskId := range input.TaskIds {
			err := setTaskNodeStatus(def.TASK_SUCCESS, taskId)
			if err != nil {
				return nil, err
			}
			_ = updateMachineCpu(taskId)
			go func(taskId uint) {
				err := mdl_node.NotifyDashboard(ctx, taskId)
				if err != nil {
					log.Warnf("operate dashboard err:%+v", err)
				}
			}(taskId)
		}
	case def.TASK_FAIL:
		for _, taskId := range input.TaskIds {
			err := setTaskNodeStatus(def.TASK_FAIL, taskId)
			if err != nil {
				return nil, err
			}
		}
	case def.TASK_CANCEL, def.TASK_NEW:
		taskList, err := tbl_task.GetListByStatus(0, def.TASK_UNRELEASE)
		if err != nil {
			log.Errorf("could not find unreleased tasks")
			return nil, err
		}
		for _, taskId := range input.TaskIds {
			for _, taskInfo := range taskList {
				if taskId == taskInfo.ID {
					log.Infof("change taskId %d status to %s", taskId, input.Status)
					if err := mdl_task.UpdateTaskStatus(taskId, input.Status); err != nil {
						return nil, err
					}
					break
				}
			}
		}
	}
	return nil, nil
}

func setTaskNodeStatus(status string, taskId uint) error {
	if err := mdl_task.Started(taskId, status); err != nil {
		log.Errorf("update task status failed.taskId:%d,err:%+v", taskId, err)
		return err
	}
	if err := mdl_node.SetNodeStatus(taskId, status); err != nil {
		log.Errorf("update node status failed.taskId:%d,err:%+v", taskId, err)
		return err
	}
	return nil
}

func updateMachineCpu(taskId uint) error {
	taskInfo, err := tbl_task.GetInfo(taskId)
	if err != nil {
		log.Warnf("get task info failed.taskId:%d", taskId)
		return err
	}
	if taskInfo.TaskExt.Operation == def.OPERATION_SUPERVISOR_STOP {
		err = mdl_resource_pool.ChangeCpuSetType(taskInfo.MachineId, []uint{taskInfo.TaskExt.ServicePort}, def.NOT_SET_CPU)
		if err != nil {
			log.Errorf("taskId:%d release cpus faild err:%v", taskInfo, err)
			return err
		}
	}
	return nil
}
