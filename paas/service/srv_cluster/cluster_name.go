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

	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type ClusterNameInput struct {
	ServiceId uint `form:"service_id"`
}

var _ servicer.Servicer = new(ClusterNameInput)

func (input *ClusterNameInput) CheckParams(ctx *gin.Context) error {
	return nil
}

func (input *ClusterNameInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	clusterNames, err := tbl_cluster.GetNamesByServiceId(input.ServiceId)
	if err != nil {
		log.Warnf("failed to get cluster names err:%v", err)
		return "", err
	}
	if len(clusterNames) <= 0 {
		log.Warn("get empty cluster names")
		return nil, err
	}
	return clusterNames, nil
}
