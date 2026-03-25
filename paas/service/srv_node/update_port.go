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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_hostport"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
)

type UpdatePortInput struct {
	ClusterId         uint `form:"clusterId"`
	GroupId           uint `form:"groupId"`
	NodeId            uint `form:"nodeId"`
	ServicePort       uint `form:"servicePort"`
	ClusterPort       uint `form:"clusterPort"`
	HostServicePortID uint `form:"hostServicePortId"`
	HostClusterPortID uint `form:"hostClusterPortId"`
}

var _ servicer.Servicer = new(UpdatePortInput)

func (input *UpdatePortInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	if input.GroupId <= 0 {
		return errors.New("invalid groupId")
	}
	if input.NodeId <= 0 {
		return errors.New("invalid nodeId")
	}
	if input.ServicePort <= 0 {
		return errors.New("invalid servicePort")
	}
	if input.ClusterPort <= 0 {
		return errors.New("invalid clusterPort")
	}
	if input.HostServicePortID <= 0 {
		return errors.New("invalid hostPortId")
	}
	return nil
}

func (input *UpdatePortInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	if err := mdl_node.Prepared(input.NodeId, input.GroupId, input.ClusterId, input.ServicePort, input.ClusterPort); err != nil {
		return nil, err
	}
	if err := tbl_hostport.Update(input.HostServicePortID, tbl_hostport.MachinePort{Port: input.ServicePort}); err != nil {
		return nil, err
	}
	if input.HostClusterPortID <= 0 {
		return nil, nil
	}
	if err := tbl_hostport.Update(input.HostClusterPortID, tbl_hostport.MachinePort{Port: input.ClusterPort}); err != nil {
		return nil, err
	}
	return nil, nil
}
