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
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
)

type MachineRegisterInput struct {
	IP  string `json:"ip"`
	IDC string `json:"idc"`
}

var _ servicer.Servicer = new(MachineRegisterInput)

func (input *MachineRegisterInput) CheckParams(ctx *gin.Context) error {
	if input.IP == "" {
		return errors.New("invalid IP")
	}
	return nil
}

func (input *MachineRegisterInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	ipInfo, err := tbl_machine.GetMachineInfo(input.IP)
	if ipInfo != nil && ipInfo.ID > 0 {
		return ipInfo.ID, nil
	}

	mid, err := tbl_machine.Register(input.IP, input.IDC, "", "", 0, 0, 0)
	if err != nil {
		return nil, err
	}
	return mid, nil
}
