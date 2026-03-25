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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type MachineUpdateInput struct {
	MachineId uint   `json:"machineId"`
	IDC       string `json:"idc"`
	Budget    string `json:"budget"`
	CpuTotal  int    `json:"cpuTotal"`
	CpuSetMax int    `json:"cpuSetMax"`
}

var _ servicer.Servicer = new(MachineUpdateInput)

func (input *MachineUpdateInput) CheckParams(ctx *gin.Context) error {
	if input.IDC <= "" {
		return errors.New("invalid idc")
	}
	if input.CpuTotal < 1 {
		return errors.New("invalid cpuTotal")
	}
	if input.CpuSetMax < 1 {
		return errors.New("invalid cpuSetMax")
	}
	if input.CpuSetMax >= input.CpuTotal {
		return errors.New("cpuSetMax < cpuTotal")
	}
	return nil
}
func (input *MachineUpdateInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	err := tbl_machine.Update(input.MachineId, tbl_machine.Machine{
		Status:    def.MACHINE_STATUS_ONLINE,
		IDC:       input.IDC,
		Budget:    input.Budget,
		CpuTotal:  input.CpuTotal,
		CpuSetMax: input.CpuSetMax,
	})
	if err != nil {
		log.Errorf("update machine failed")
		return nil, err
	}
	return nil, nil
}
