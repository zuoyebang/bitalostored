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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_group"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type DeleteOfflineInput struct {
	ClusterId uint `form:"clusterId"`
}

var _ servicer.Servicer = new(DeleteOfflineInput)

func (input *DeleteOfflineInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	return nil
}

func (input *DeleteOfflineInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	clusterInfo, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil {
		log.Warnf("get cluster info failed.err:%+v", err)
		return nil, err
	}
	if def.IsServer(clusterInfo.ServiceId) {
		groupList, err := tbl_group.GetList(input.ClusterId, def.GROUP_STATUS_ONLINE, -1, 0)
		if err != nil {
			log.Warnf("get cluster group list failed.err:%+v", err)
			return nil, err
		}
		for _, group := range groupList {
			list, err := tbl_node.GetOnlineListByGroup(input.ClusterId, group.GroupId, -1, 0)
			if err != nil {
				msg := fmt.Sprintf("query tblNode records failed, clusterId:%d groupId:%d", input.ClusterId, group.GroupId)
				return nil, errors.New(msg)
			}
			if len(list) == 0 {
				err = tbl_node.DeleteByGroup(input.ClusterId, group.GroupId)
				if err != nil {
					log.Warnf("delete group nodes failed.err:%+v.clusterId:%d,groupId:%d", err, input.ClusterId, group.GroupId)
				}
				err = tbl_group.Delete(group.ClusterId, group.GroupId)
				if err != nil {
					log.Warnf("delete group failed.err:%+v.clusterId:%d,groupId:%d", err, input.ClusterId, group.GroupId)
				}
				err = tbl_task.DeleteByGroup(group.ClusterId, group.GroupId)
				if err != nil {
					log.Warnf("delete group task failed.err:%+v.clusterId:%d,groupId:%d", err, input.ClusterId, group.GroupId)
				}
			} else {
				maxNodeId, err := tbl_node.DeleteLittleNodes(input.ClusterId, group.GroupId)
				if err != nil {
					log.Warn("delete little nodes failed.err:", err, " groupId:", group.GroupId, " clusterId:", input.ClusterId)
				}
				if maxNodeId > 0 {
					//tbl_task.DeleteLittleNode(input.ClusterId, group.GroupId, maxNodeId)
				}
				log.Info("group:", group.GroupId, " is not full offline. online list:", len(list))
			}
		}
		return nil, nil
	}
	if clusterInfo.ServiceId == def.SERVICE_ID_PROXY {
		return nil, tbl_node.DeleteClusterOfflineNode(input.ClusterId)
	}
	return nil, errors.New("unsupported service type.")
}
