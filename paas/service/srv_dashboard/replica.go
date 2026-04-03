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

package srv_dashboard

import (
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/web/dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/redis_op"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type ReplicaInput struct {
	ClusterId uint `json:"clusterId"`
	GroupId   uint `json:"groupId"`
	NodeId    uint `json:"nodeId"`
	Replica   bool `json:"replica"`
}

var _ servicer.Servicer = new(ReplicaInput)

func (input *ReplicaInput) CheckParams(ctx *gin.Context) error {
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
func (input *ReplicaInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	nodeInfo, err := tbl_node.GetInfo(input.NodeId, input.GroupId, input.ClusterId)
	if err != nil {
		log.Warn("get node info failed.err:", err)
		return nil, err
	}
	if nodeInfo.IsWitness {
		return nil, errors.New("witness node. Replica operation unsupported!")
	}
	machineInfo, err := tbl_machine.GetInfo(nodeInfo.MachineId)
	if err != nil {
		log.Warn("get machine info failed.err:", err)
		return nil, err
	}
	clusterInfo, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil {
		log.Warn("get cluster info failed.err:", err)
		return nil, err
	}
	var replica uint = 0
	if input.Replica {
		replica = 1
	}
	replicaServer := fmt.Sprintf("%s:%d", machineInfo.IP, nodeInfo.ServicePort)
	if !input.Replica && redis_op.MayBeNodeMaster(replicaServer, input.ClusterId, input.GroupId, clusterInfo.Name) {
		return nil, errors.New("could not shutdown master node")
	}
	if err := dashboard.SetDashboardCookie(ctx); err != nil {
		return nil, err
	}
	return nil, dashboard.ReplicaNode(ctx, replicaServer, clusterInfo.Name, replica, input.GroupId)
}
