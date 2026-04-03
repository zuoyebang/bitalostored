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
	"github.com/zuoyebang/bitalostored/paas/model/mdl_cluster"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/service/srv_node"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type MarkOfflineInput struct {
	ClusterId uint `form:"clusterId"`
}

var _ servicer.Servicer = new(MarkOfflineInput)

func (input *MarkOfflineInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	return nil
}

func (input *MarkOfflineInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	clusterInfo, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil {
		log.Warnf("get cluster fail err:%v", err)
		return nil, err
	}
	dashboardName, err := mdl_dashboard.GetDashboardName(clusterInfo.StoredId)
	if err != nil {
		log.Warn("get dashboard name failed.err:", err)
		dashboardName = clusterInfo.Name
	}
	replicas, err := srv_node.GetReplica(dashboardName)
	if err != nil {
		log.Warn("get replica failed.err:", err)
		return nil, err
	}
	replicaGroupIds := make(map[uint]map[string]string, 0)
	if def.ServciceIdIsServer(clusterInfo.ServiceId) {
		replicaGroupIds, err = mdl_node.GetSlotsGroupIds(clusterInfo.Name, def.SERVICE_ID_PROXY)
		if err != nil {
			return nil, errors.New(err.Error())
		}
	}

	_, err = mdl_cluster.OfflineClusterNodes(input.ClusterId, clusterInfo.ServiceId, replicas, replicaGroupIds)
	return nil, err
}
