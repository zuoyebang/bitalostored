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
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strings"
)

type MachineMultiRemoveInput struct {
	IP string `json:"ip"`
}

var _ servicer.Servicer = new(MachineMultiRemoveInput)

func (input *MachineMultiRemoveInput) CheckParams(ctx *gin.Context) error {
	if len(input.IP) == 0 {
		return errors.New("invalid ip")
	}
	return nil
}

func (input *MachineMultiRemoveInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	ips := strings.Split(input.IP, "\n")
	deleteMids := make([]uint, 0, len(ips))
	for _, ip := range ips {
		ip = strings.TrimSpace(ip)
		if len(ip) == 0 {
			continue
		}
		mInfo, err := tbl_machine.GetMachineInfo(ip)
		if err != nil {
			continue
		}
		if mInfo.Status != def.MACHINE_STATUS_OFFLINE {
			continue
		}
		deleteMids = append(deleteMids, mInfo.ID)
	}
	if len(deleteMids) == 0 {
		return nil, nil
	}
	if err := tbl_regionmachine.MultiDeleteMachines(deleteMids); err != nil {
		log.Infof("delete region machine err. machineIds:%d", deleteMids)
	}
	if err := tbl_hostport.MultiDeleteByMachine(deleteMids); err != nil {
		log.Infof("delete host port err. machineIds:%d", deleteMids)
	}
	tbl_machine.MultiDeleteMachine(deleteMids)
	return nil, nil
}
