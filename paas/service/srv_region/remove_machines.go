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

package srv_region

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_regionmachine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type RemoveRegionMachinesInput struct {
	RegionId      uint   `json:"regionId"`
	MachineIdList []uint `json:"machineIdList"`
}

var _ servicer.Servicer = new(RemoveRegionMachinesInput)

func (input *RemoveRegionMachinesInput) CheckParams(ctx *gin.Context) error {
	if input.RegionId <= 0 {
		return errors.New("invalid regionId")
	}
	return nil
}

func (input *RemoveRegionMachinesInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	nodeList, err := tbl_node.GetListByMachineRegion(input.MachineIdList, input.RegionId)
	if err != nil {
		log.Warn("get node list failed.err:", err)
		return nil, err
	}
	feService, _ := tbl_service.GetInfoByName(def.SERVICE_STORED_FE)
	dashboardService, _ := tbl_service.GetInfoByName(def.SERVICE_STORED_DASHBOARD)
	failedMachine := make(map[uint]bool, 0)
	var successMachine []uint
	for _, node := range nodeList {
		if failedMachine[node.MachineId] {
			continue
		}
		if node.ServiceId == feService.ID || node.ServiceId == dashboardService.ID {
			if node.Status == "offline" {
				continue
			}
			log.Info("skip the machine having online dashboard.serviceId:", node.ServiceId, " machineId:", node.MachineId)
			failedMachine[node.MachineId] = true
			continue
		}
		nodeStatus, err := mdl_node.UpdateOfflineNode(node)
		if err != nil {
			log.Warn("offline cluster nodes failed.err:", err)
			return nil, err
		}
		if nodeStatus == 0 {
			failedMachine[node.MachineId] = true
			return nil, errors.New("still got alive nodes in this machine. Could not remove the region machine.")
		}
	}
	for _, machineId := range input.MachineIdList {
		if !failedMachine[machineId] {
			successMachine = append(successMachine, machineId)
		}
	}
	if len(successMachine) > 0 {
		err := tbl_regionmachine.DeleteRegionMachines(input.RegionId, successMachine)
		if err != nil {
			log.Errorf("unbind region machine failed.err:%+v", err)
			return nil, err
		}
		log.Infof("delete machines %+v from region %d", successMachine, input.RegionId)
	}
	if len(failedMachine) > 0 {
		return nil, errors.New(fmt.Sprintf("still got online service in machines %+v", failedMachine))
	}
	return nil, nil
}
