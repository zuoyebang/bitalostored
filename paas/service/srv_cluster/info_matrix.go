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

package srv_cluster

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_group"
	"github.com/zuoyebang/bitalostored/paas/dao/web/dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type MatrixInfoInput struct {
	ClusterId uint `form:"clusterId"`
}

var _ servicer.Servicer = new(MatrixInfoInput)

func (input *MatrixInfoInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	return nil
}

func (input *MatrixInfoInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	var output MatrixInfoOutput
	var err error
	output.ClusterInfo, err = tbl_cluster.GetInfo(input.ClusterId)
	if err != nil {
		log.Warnf("failed to get cluster info.clusterId:%d.err:%+v", input.ClusterId, err)
		return nil, err
	}
	groupInfos, err := tbl_group.GetList(input.ClusterId, "", -1, 0)
	if err != nil {
		log.Warnf("failed to get cluster groups.err:%+v", err)
		return nil, err
	}
	dashboardName, err := mdl_dashboard.GetDashboardName(output.ClusterInfo.StoredId)
	if err != nil || len(dashboardName) <= 0 {
		log.Warn("get dashboard name failed.err:", err)
		dashboardName = output.ClusterInfo.Name
	}
	topom, err := dashboard.GetTopom(dashboardName)
	if err != nil {
		log.Warnf("failed to get dashboard topom.address:%s.cluster name:%s.err:%+v", config.GetConf().Domains.DashboardDomain, output.ClusterInfo.Name, err)
	}
	replicaMap := make(map[string]bool)
	if topom != nil {
		for _, f := range topom.Data.Stats.Group.Models {
			for _, s := range f.Servers {
				replicaMap[s.Addr] = s.ReplicaGroup
			}
		}
	}
	groupServiceId := output.ClusterInfo.ServiceId
	for _, group := range groupInfos {
		if group.ServiceId > 0 {
			groupServiceId = group.ServiceId
		}
		nodeInfos, err := mdl_node.GetMatrixNodeList(output.ClusterInfo, group.GroupId, groupServiceId, group.Lock, replicaMap)
		if err != nil {
			log.Warnf("failed to get group nodes.err:%+v", err)
			return nil, err
		}
		output.GroupInfos = append(output.GroupInfos, GroupsInfo{NodeInfos: nodeInfos, Group: group})
	}
	return output, nil
}

type MatrixInfoOutput struct {
	ClusterInfo *tbl_cluster.Cluster `json:"clusterInfo"`
	GroupInfos  []GroupsInfo         `json:"rows"`
}

type GroupsInfo struct {
	*tbl_group.Group
	NodeInfos []mdl_node.MdlNode `json:"nodeInfos"`
}
