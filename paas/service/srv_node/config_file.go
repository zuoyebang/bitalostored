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
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type NodeConfigInput struct {
	ClusterId uint `form:"clusterId"`
	GroupId   uint `form:"groupId"`
	NodeId    uint `form:"nodeId"`
}

var _ servicer.Servicer = new(NodeConfigInput)

func (input *NodeConfigInput) CheckParams(ctx *gin.Context) error {
	if input.GroupId <= 0 {
		return errors.New("invalid groupId")
	}
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	if input.NodeId <= 0 {
		return errors.New("invalid nodeId")
	}
	return nil
}

func (input *NodeConfigInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	nodeInfo, err := tbl_node.GetInfo(input.NodeId, input.GroupId, input.ClusterId)
	if err != nil {
		log.Warn("get node info failed.err:", err)
		return "", err
	}
	return nodeInfo.ConfigContent, nil
}
