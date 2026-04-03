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
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_machine"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strings"
)

type MachineMarkOfflineInput struct {
	IP string `json:"ip"`
}

var _ servicer.Servicer = new(MachineMarkOfflineInput)

func (input *MachineMarkOfflineInput) CheckParams(ctx *gin.Context) error {
	if len(input.IP) == 0 {
		return errors.New("invalid ips")
	}
	return nil
}

func (input *MachineMarkOfflineInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	ips := strings.Split(input.IP, "\n")
	offlineIds := make([]uint, 0, len(ips))
	for _, ip := range ips {
		ip = strings.TrimSpace(ip)
		if len(ip) == 0 {
			continue
		}
		machineInfo, err := tbl_machine.GetMachineInfo(ip)
		if err != nil {
			continue
		}
		if err := checkAllNodeOffline(machineInfo.ID); err != nil {
			continue
		}
		offlineIds = append(offlineIds, machineInfo.ID)
	}
	return nil, mdl_machine.MultiOffline(offlineIds)
}

func checkAllNodeOffline(machineId uint) error {
	nodeList, err := tbl_node.GetListByMachine(machineId, -1, 0)
	if err != nil {
		log.Warn("get nodelist failed.err:", err)
		return err
	}
	for _, node := range nodeList {
		if node.Status == def.NODE_STATUS_OFFLINE {
			continue
		} else {
			return errors.New("exist alive node.")
		}
	}
	return nil
}
