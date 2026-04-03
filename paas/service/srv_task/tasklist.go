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
	"github.com/zuoyebang/bitalostored/paas/model/mdl_task"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
)

type TaskListInput struct {
	MachineId uint     `form:"machineId"`
	Status    []string `form:"status"`
	Type      []string `form:"type"`
	Limit     int      `form:"limit"`
	Offset    int      `form:"offset"`
}

func (input *TaskListInput) CheckParams(ctx *gin.Context) error {
	if len(input.Status) == 0 {
		input.Status = []string{def.TASK_NEW}
	}
	if input.Offset <= 0 {
		input.Offset = 0
	}
	if input.Limit <= 0 || input.Limit > 100 {
		input.Limit = 10
	}
	return nil
}

func (input *TaskListInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	return mdl_task.GetList(input.MachineId, input.Type, input.Status, input.Limit, input.Offset)
}
