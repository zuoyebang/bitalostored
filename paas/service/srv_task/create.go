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

import "github.com/gin-gonic/gin"

type TaskCreateInput struct {
	Type string `json:"type"`

	RegionId  uint `json:"regionId"`
	MachineId uint `json:"machineId"`
	ServiceId uint `json:"serviceId"`
	PackageId uint `json:"packageId"`

	ServiceName      string `json:"serviceName"`
	ServicePortRange []int  `json:"servicePortRange"`
	ClusterPortRange []int  `json:"clusterPortRange"`
}

func (input *TaskCreateInput) CheckParams(ctx *gin.Context) error {
	return nil
}
func (input *TaskCreateInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	// res, err := mdl_task.Create(input.Type, input.RegionId, input.MachineId, input.ServiceId, "{}")
	// if err != nil {
	// 	return nil, err
	// }
	// err = mdl_task.Prepare(res.ID, input.ServiceName, input.ServicePortRange, input.ClusterPortRange)
	// if err != nil {
	// 	return nil, err
	// }
	// return &TaskAddOutput{TaskId: res.ID}, nil
	return nil, nil
}

type TaskAddOutput struct {
	TaskId uint `json:"taskId"`
}
