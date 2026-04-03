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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_machine"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type MachineOfflineInput struct {
	MachineId uint `json:"machineId"`
}

var _ servicer.Servicer = new(MachineOfflineInput)

func (input *MachineOfflineInput) CheckParams(ctx *gin.Context) error {
	if input.MachineId == 0 {
		return errors.New("invalid machineId")
	}
	return nil
}
func (input *MachineOfflineInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	nodeList, err := tbl_node.GetListByMachine(input.MachineId, -1, 0)
	if err != nil {
		log.Warn("get nodelist failed.err:", err)
		return nil, err
	}
	var failedNodeId []int
	for _, node := range nodeList {
		if node.Status == def.NODE_STATUS_OFFLINE {
			continue
		}
		status, err := mdl_node.UpdateOfflineNode(node)
		if err != nil {
			log.Warn("update node failed.err:", err)
			failedNodeId = append(failedNodeId, int(node.NodeId))
			continue
		}
		if status == 0 {
			failedNodeId = append(failedNodeId, int(node.NodeId))
		}
	}
	if len(failedNodeId) > 0 {
		return failedNodeId, errors.New("exist alive node.")
	}
	return nil, mdl_machine.Offline(input.MachineId)
}
