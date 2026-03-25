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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type RemoveClusterInput struct {
	ClusterId uint `json:"clusterId"`
}

var _ servicer.Servicer = new(RemoveClusterInput)

func (input *RemoveClusterInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	return nil
}

func (input *RemoveClusterInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	err := tbl_cluster.Delete(input.ClusterId)
	if err != nil {
		log.Error("delete cluster failed.err:", err)
		return nil, err
	}
	err = tbl_group.DeleteByCluster(input.ClusterId)
	if err != nil {
		log.Error("delete groups failed.err:", err)
		return nil, err
	}
	err = tbl_node.DeleteByCluster(input.ClusterId)
	if err != nil {
		log.Error("delete nodes failed.err:", err)
		return nil, err
	}
	err = tbl_task.DeleteByCluster(input.ClusterId)
	if err != nil {
		log.Error("delete tasks failed.err:", err)
		return nil, err
	}
	return nil, nil
}
