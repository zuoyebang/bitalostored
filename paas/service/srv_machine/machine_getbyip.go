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

type MachineGetByIpInput struct {
	IP string `json:"ip"`
}

var _ servicer.Servicer = new(MachineGetByIpInput)

func (input *MachineGetByIpInput) CheckParams(ctx *gin.Context) error {
	if input.IP == "" {
		return errors.New("invalid IP")
	}
	return nil
}

func (input *MachineGetByIpInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	info, _ := tbl_machine.GetMachineInfo(input.IP)
	return info, nil
}
