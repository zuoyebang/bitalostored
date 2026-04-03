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
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_dashboard"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type OfflineInput struct {
	ClusterId uint `json:"clusterId"`
}

var _ servicer.Servicer = new(OfflineInput)

func (input *OfflineInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	return nil
}

func (input *OfflineInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	clusterInfo, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil {
		log.Warnf("get cluster info failed.err:%+v", err)
		return nil, err
	}

	nodes, err := tbl_node.GetListByCluster(input.ClusterId)
	if err != nil {
		log.Warnf("get nodes info failed.err:%+v", err)
		return nil, err
	}

	if clusterInfo.ServiceId == def.SERVICE_ID_DASHBOARD {
		for _, n := range nodes {
			if n.Status == def.NODE_STATUS_OFFLINE {
				continue
			}
			alive, err := mdl_node.CheckDashboardAlive(n)
			if err != nil {
				return nil, err
			}
			if alive {
				return nil, errors.New("node is alive")
			}

			err = tbl_node.DeleteNode(n.NodeId, n.GroupId, n.ClusterId)
			if err != nil {
				return nil, errors.New(fmt.Sprintf("delete node record failed: err:%v", err))
			}
		}
		clusterName := clusterInfo.Name
		clusterList, err := tbl_cluster.GetListByNs(clusterName, def.SERVICE_ID_DASHBOARD)
		if err != nil {
			return nil, err
		}
		isLast := true
		for _, c := range clusterList {
			if c.Status == def.CLUSTER_STATUS_OFFLINE {
				continue
			}
			if c.Id == input.ClusterId {
				continue
			} else {
				isLast = false
				break
			}
		}
		if isLast {
			err = tbl_dashboard.DeleteCluster(clusterName)
			log.Infof("cluster will be deleted, clear tblDashboard records. cluster:%s result:%s", clusterName, err)
			if err != nil {
				return nil, err
			}
		}
		err = tbl_cluster.Delete(clusterInfo.Id)
		return nil, err
	}

	var failedNodeId []int
	for _, n := range nodes {
		status, err := mdl_node.UpdateOfflineNode(n)
		if err != nil {
			log.Warn("update node failed.err:", err)
			failedNodeId = append(failedNodeId, int(n.NodeId))
			continue
		}
		if status == 0 {
			failedNodeId = append(failedNodeId, int(n.NodeId))
		}
	}
	if len(failedNodeId) > 0 {
		return nil, errors.New(fmt.Sprintf("offline nodes failed.failed nodes:%v", failedNodeId))
	}
	err = tbl_cluster.Update(clusterInfo.Id, tbl_cluster.Cluster{Status: def.CLUSTER_STATUS_OFFLINE})
	return nil, err
}
