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
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_task"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type TaskStartedInput struct {
	TaskId uint   `json:"taskId"`
	Status string `json:"status"`
	Errors string `json:"errors"`
}

func (input *TaskStartedInput) CheckParams(ctx *gin.Context) error {
	return nil
}
func (input *TaskStartedInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	if err := mdl_task.Started(input.TaskId, input.Status); err != nil {
		log.Errorf("update task status failed.taskId:%d,err:%+v", input.TaskId, err)
		return nil, err
	}
	if err := mdl_node.SetNodeStatus(input.TaskId, input.Status); err != nil {
		log.Errorf("update node status failed.taskId:%d,err:%+v", input.TaskId, err)
		return nil, err
	}
	if input.Status == def.TASK_FAIL {
		log.Info("taskID:", input.TaskId, " failed")
		return nil, nil
	}
	if err := mdl_node.NotifyDashboard(ctx, input.TaskId); err != nil {
		return nil, err
	}
	return nil, nil
}
