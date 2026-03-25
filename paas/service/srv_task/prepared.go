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
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_ops"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_task"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type TaskPreparedInput struct {
	TaskId      uint `json:"taskId"`
	NodeId      uint `json:"nodeId"`
	ServicePort uint `json:"servicePort"`
	ClusterPort uint `json:"clusterPort"`
}

func (input *TaskPreparedInput) CheckParams(ctx *gin.Context) error {
	if input.ServicePort <= 0 || input.ClusterPort <= 0 {
		return errors.New("port empty")
	}
	return nil
}
func (input *TaskPreparedInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	t, err := tbl_task.GetInfo(input.TaskId)
	if err != nil {
		return nil, err
	}
	if err := mdl_task.UpdatePorts(input.TaskId, input.ServicePort, input.ClusterPort); err != nil {
		log.Warn("update task failed.")
		return nil, err
	}
	if err := mdl_node.Prepared(input.NodeId, t.GroupId, t.ClusterId, input.ServicePort, input.ClusterPort); err != nil {
		return nil, err
	}

	extString := mdl_resource_pool.FormatMachineCgroup(input.ServicePort, t.ClusterId, t.MachineId, t.TaskExt.CloudType)
	if t.Type == def.TASK_TYPE_PREPARESTART {
		if _, err := mdl_task.CheckGroup(t, extString); err != nil {
			return nil, err
		}
		return nil, nil
	}

	t.Type = def.TASK_TYPE_ADD
	t.Status = def.TASK_NEW
	t.TaskExt.ExtString = extString
	t.TaskExt.ServicePort = input.ServicePort
	t.TaskExt.ClusterPort = input.ClusterPort
	t.TaskExt.NodeListVal = fmt.Sprintf("%s:%d", t.TaskExt.Ip, input.ClusterPort)
	e := tbl_task.UpdateTask(t.ID, t)
	if e != nil {
		return nil, e
	}

	mdl_ops.CreateOpsActionLog(&tbl_node.Node{}, t)

	return nil, err
}
