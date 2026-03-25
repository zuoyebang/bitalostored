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

package srv_agent

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cosfile"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_regionmachine"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type UpgradeAgentInput struct {
	RegionId      uint   `json:"regionId"`
	MachineIdList []uint `json:"machineIdList"`
	CosFileId     uint   `json:"packageId"`
	AgentConfig   string `json:"agentConfig"`
}

var _ servicer.Servicer = new(UpgradeAgentInput)

func (input *UpgradeAgentInput) CheckParams(ctx *gin.Context) error {
	if input.RegionId <= 0 && len(input.MachineIdList) == 0 {
		return errors.New("invalid param regionId , machineIdList")
	}
	if input.CosFileId == 0 {
		return errors.New("invalid version")
	}
	return nil
}

func (input *UpgradeAgentInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	if len(input.MachineIdList) == 0 {
		var err error
		input.MachineIdList, err = tbl_regionmachine.GetMachinesByRegion(input.RegionId)
		if err != nil {
			log.Warnf("failed to get region machines.err:%+v", err)
			return nil, err
		}
	}
	info, err := tbl_cosfile.GetCosFile(input.CosFileId)
	if err != nil || info == nil {
		log.Warn("get cos file failed.err:", err)
		return nil, err
	}
	for _, mID := range input.MachineIdList {
		if err := tbl_machine.Update(mID, tbl_machine.Machine{
			NeedUpgrade:      def.NEED_UPGRADE_YES,
			UpgradeVersionId: input.CosFileId,
			UpgradeConfig:    input.AgentConfig,
		}); err != nil {
			log.Warn("set need upgrade status failed.err:", err)
			return nil, err
		}
	}
	return nil, nil
}
