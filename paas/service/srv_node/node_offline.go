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

package srv_node

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strconv"
)

type OfflineNodeInput struct {
	ClusterId uint `json:"clusterId"`
	GroupId   uint `json:"groupId"`
	NodeId    uint `json:"nodeId"`
}

var _ servicer.Servicer = new(OfflineNodeInput)

func (input *OfflineNodeInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	if input.GroupId <= 0 {
		return errors.New("invalid groupId")
	}
	if input.NodeId <= 0 {
		return errors.New("invalid nodeId")
	}
	return nil
}

func (input *OfflineNodeInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	node, err := tbl_node.GetInfo(input.NodeId, input.GroupId, input.ClusterId)
	if err != nil {
		log.Warnf("get node err:%v", err)
		return nil, err
	}
	if node.ServiceId != def.SERVICE_ID_BITALOS {
		return nil, fmt.Errorf("node type is not server")
	}
	minfo, err := tbl_machine.GetInfo(node.MachineId)
	if err != nil {
		log.Warnf("get machine err:%v", err)
		return nil, err
	}
	p := strconv.Itoa(int(node.ServicePort))
	addr := minfo.IP + ":" + p
	err = mdl_node.MarkServerNodeOffline(addr, input.NodeId, input.GroupId, input.ClusterId)
	return nil, err
}
