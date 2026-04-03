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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_hostport"
)

type TaskHostPortInput struct {
	MachineId uint   `json:"machineId"`
	Port      uint   `json:"port"`
	IP        string `json:"ip"`
}

func (input *TaskHostPortInput) CheckParams(ctx *gin.Context) error {
	return nil
}
func (input *TaskHostPortInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	data, err := tbl_hostport.Create(input.MachineId, input.Port, input.IP)
	if err != nil {
		return nil, err
	}
	return data.ID, nil
}
