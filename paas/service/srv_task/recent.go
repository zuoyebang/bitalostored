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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"strconv"
	"time"
)

type RecentTasksInput struct {
	ClusterId    uint  `form:"clusterId"`
	TimeInterval int64 `form:"timeInterval"`
	Limit        int   `form:"limit"`
	Offset       int   `form:"offset"`
}

var _ servicer.Servicer = new(RecentTasksInput)

func (input *RecentTasksInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	if input.Limit <= 0 {
		input.Limit = -1
		input.Offset = 0
	}
	if input.TimeInterval <= 0 || input.TimeInterval >= time.Now().Unix() {
		input.TimeInterval = 60 * 60 * 24
	}
	return nil
}

func (input *RecentTasksInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	output := RecentTasksOutput{}
	updateTime := time.Now().Unix() - input.TimeInterval
	taskList, err := tbl_task.GetListByClusterId(input.ClusterId, updateTime, input.Limit, input.Offset)
	if err != nil {
		log.Error("getTaskList error.err:", err.Error())
		return nil, err
	}
	output.Rows = taskListToTaskInfoList(taskList)
	output.Count = tbl_task.Count(input.ClusterId, updateTime)
	return output, nil
}

type RecentTasksOutput struct {
	Rows  []*TaskInfo `json:"rows"`
	Count int64       `json:"count"`
}

type TaskInfo struct {
	TaskId      uint        `json:"taskId"`
	TaskType    string      `json:"taskType"`
	TaskStatus  string      `json:"taskStatus"`
	Server      string      `json:"server"` // ip:port
	Service     string      `json:"mdl_service"`
	ClusterId   uint        `json:"clusterId"`
	GroupId     uint        `json:"groupId"`
	NodeId      uint        `json:"nodeId"`
	Extra       interface{} `json:"extra"`
	MigrateInfo string      `json:"migrateInfo"`
	CreateTime  string      `json:"createTime"`
	UpdateTime  string      `json:"updateTime"`
}

func taskListToTaskInfoList(taskList []*tbl_task.Task) []*TaskInfo {
	ti := []*TaskInfo{}
	for _, t := range taskList {
		info := TaskInfo{}
		info.TaskId = t.ID
		info.TaskType = t.Type
		info.Extra = t.Extra
		info.CreateTime = math2.UnixTimeToStr(t.CreateTime)
		info.UpdateTime = math2.UnixTimeToStr(t.UpdateTime)
		info.TaskStatus = t.Status
		info.ClusterId = t.ClusterId
		info.GroupId = t.GroupId
		info.NodeId = t.NodeId
		info.Server = t.TaskExt.Ip + ":" + strconv.Itoa(int(t.TaskExt.ServicePort))
		info.Service = t.TaskExt.ServiceName
		info.MigrateInfo = t.TaskExt.MigratedFromInfo
		ti = append(ti, &info)
	}
	return ti
}
