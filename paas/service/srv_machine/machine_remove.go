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

package srv_machine

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_hostport"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_regionmachine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
)

type MachineRemoveInput struct {
	MachineId uint `json:"machineId"`
}

var _ servicer.Servicer = new(MachineRemoveInput)

func (input *MachineRemoveInput) CheckParams(ctx *gin.Context) error {
	if input.MachineId == 0 {
		return errors.New("invalid machineId")
	}
	return nil
}

func (input *MachineRemoveInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	mInfo, err := tbl_machine.GetInfo(input.MachineId)
	if err != nil {
		return nil, err
	}
	if mInfo.Status != def.MACHINE_STATUS_OFFLINE {
		return nil, errors.New("can only remove a offline machine.")
	}
	if err := tbl_regionmachine.DeleteByMachine(input.MachineId); err != nil {
		return nil, err
	}
	if err := tbl_hostport.DeleteByMachine(input.MachineId); err != nil {
		return nil, err
	}

	if err := tbl_task.DeleteByMachine(input.MachineId); err != nil {
		return nil, err
	}
	return nil, tbl_machine.DeleteMachine(input.MachineId)
}
