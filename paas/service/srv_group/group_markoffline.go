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

package srv_group

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
)

type MarkOfflineGroupInput struct {
	ClusterId uint `json:"clusterId"`
	GroupId   uint `json:"groupId"`
}

var _ servicer.Servicer = new(MarkOfflineGroupInput)

func (input *MarkOfflineGroupInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	if input.GroupId <= 0 {
		return errors.New("invalid groupId")
	}
	return nil
}

func (input *MarkOfflineGroupInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	clusterInfo, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil {
		return nil, err
	}
	nodeList, err := mdl_node.GetDataNodeList(clusterInfo, input.GroupId)
	if err != nil {
		return nil, err
	}
	for _, nd := range nodeList {
		nodeAddr := fmt.Sprintf("%s:%d", nd.IP, nd.ServicePort)
		mdl_node.MarkServerNodeOffline(nodeAddr, nd.NodeId, nd.GroupId, nd.ClusterId)
	}
	return nil, nil
}
