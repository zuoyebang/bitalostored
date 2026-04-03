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
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type AgentManageInfoInput struct {
	MachineId uint   `form:"machineId"`
	Version   string `form:"version"`
}

var _ servicer.Servicer = new(AgentManageInfoInput)

func (input *AgentManageInfoInput) CheckParams(ctx *gin.Context) error {
	if input.MachineId <= 0 {
		return errors.New("invalid machineId")
	}
	if input.Version == "" {
		return errors.New("invalid version")
	}
	return nil
}

func (input *AgentManageInfoInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	mInfo, err := tbl_machine.GetInfo(input.MachineId)
	if err != nil {
		log.Warn("get machine info failed.err:", err)
		return AgentManageInfoOutput{}, err
	}
	if mInfo.NeedUpgrade == def.NEED_UPGRADE_YES {
		info, err := tbl_cosfile.GetCosFile(mInfo.UpgradeVersionId)
		if err != nil {
			log.Warn("get cos file failed.err:", err)
			return nil, err
		}
		if mInfo.UpgradeConfig == "" || mInfo.UpgradeConfig == mInfo.AgentConfig {
			return AgentManageInfoOutput{CosKey: info.CosKey}, tbl_machine.Update(input.MachineId, tbl_machine.Machine{NeedUpgrade: def.NEED_UPGRADE_NO})
		}
		return AgentManageInfoOutput{CosKey: info.CosKey, Content: mInfo.UpgradeConfig}, tbl_machine.Update(input.MachineId, tbl_machine.Machine{NeedUpgrade: def.NEED_UPGRADE_NO, AgentConfig: mInfo.UpgradeConfig})
	}
	return AgentManageInfoOutput{}, tbl_machine.Update(input.MachineId, tbl_machine.Machine{Version: input.Version})
}

type AgentManageInfoOutput struct {
	CosKey  string `json:"cosKey"`
	Content string `json:"content"`
}
