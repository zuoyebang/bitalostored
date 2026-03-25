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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_machine"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"

	"github.com/gin-gonic/gin"
)

type MachineRemoveProxyInput struct {
	IP string `json:"ip"`
}

var _ servicer.Servicer = new(MachineRemoveProxyInput)

func (input *MachineRemoveProxyInput) CheckParams(ctx *gin.Context) error {
	if len(input.IP) == 0 {
		return errors.New("invalid machine ip")
	}
	return nil
}

func (input *MachineRemoveProxyInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	machineInfo, err := tbl_machine.GetMachineInfo(input.IP)
	if err != nil {
		return nil, err
	}
	if machineInfo.Status == def.MACHINE_STATUS_OFFLINE {
		return nil, nil
	}
	if machineInfo.IP != input.IP {
		return nil, errors.New("ip not matched")
	}
	nodeList, err := tbl_node.GetListByMachine(machineInfo.ID, -1, 0)
	if err != nil {
		log.Warn("get nodelist failed.err:", err)
		return nil, err
	}
	for _, node := range nodeList {
		if node.Status == def.NODE_STATUS_OFFLINE {
			continue
		}
		if node.ServiceId != def.SERVICE_ID_PROXY {
			continue
		}
		if err := mdl_machine.StopAndRemoveProxy(ctx, machineInfo, node); err != nil {
			log.Errorf("remove proxy fail. node:%s:%d err:%s", machineInfo.IP, node.ServicePort, err)
		}
		// break // debug
	}
	return nil, nil
}
