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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
)

type HistoryTasksInput struct {
	ClusterId uint `form:"clusterId"`
	NodeId    uint `form:"nodeId"`
	GroupId   uint `form:"groupId"`
	Page      int  `form:"page"`
	Num       int  `form:"num"`
}

type HistoryRow struct {
	Address       string `json:"address"`
	Version       string `json:"version"`
	Type          string `json:"type"`
	Status        string `json:"status"`
	OperationTime string `json:"operationTime"`
}

var _ servicer.Servicer = new(HistoryTasksInput)

func (input *HistoryTasksInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	if input.NodeId <= 0 {
		return errors.New("invalid nodeId")
	}
	if input.GroupId <= 0 {
		return errors.New("invalid groupId")
	}
	if input.Page == 0 {
		input.Page = 1
	}
	if input.Num == 0 {
		input.Num = 20
	}
	return nil
}

func (input *HistoryTasksInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	output := HistoryTasksOutput{}
	taskList, err := tbl_task.GetHistoryList(input.ClusterId, input.GroupId, input.NodeId, input.Page, input.Num)
	if err != nil {
		log.Error("getTaskList error.err:", err.Error())
		return nil, err
	}
	output.Rows = make([]*HistoryRow, 0, len(taskList))
	for _, task := range taskList {
		output.Rows = append(output.Rows, formatRows(task))
	}
	output.Count = tbl_task.GetHistoryCount(input.ClusterId, input.GroupId, input.NodeId)
	return output, nil
}

func formatRows(t *tbl_task.Task) *HistoryRow {
	opType := "unknown"
	switch t.Type {
	case def.TASK_TYPE_START:
		opType = fmt.Sprintf("%s(%s)", t.Type, "creeate")
	case def.TASK_TYPE_ADD:
		opType = fmt.Sprintf("%s(%s)", t.Type, "add")
	case def.TASK_TYPE_OPERATE:
		if t.TaskExt.Operation == def.OPERATION_SUPERVISOR_START {
			opType = fmt.Sprintf("%s(%s)", t.Type, "start")
		}
		if t.TaskExt.Operation == def.OPERATION_BITALOS_UPGRADE {
			opType = fmt.Sprintf("%s(%s)", t.Type, "upgrade")
		}
		if t.TaskExt.Operation == def.OPERATION_SUPERVISOR_STOP {
			opType = fmt.Sprintf("%s(%s)", t.Type, "stop")
		}
	case def.TASK_TYPE_UPGRADE:
		opType = fmt.Sprintf("%s(%s)", t.Type, "upgrade")
	}
	address := fmt.Sprintf("%s:%d", t.TaskExt.Ip, t.TaskExt.ServicePort)
	r := &HistoryRow{
		Address:       address,
		Type:          opType,
		Version:       t.CosFileVersion,
		Status:        t.Status,
		OperationTime: math2.UnixTimeToStr(t.CreateTime),
	}
	return r
}

type HistoryTasksOutput struct {
	Rows  []*HistoryRow `json:"rows"`
	Count int64         `json:"count"`
}
