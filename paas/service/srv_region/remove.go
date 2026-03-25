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

package srv_region

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_regionmachine"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_cluster"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type RemoveRegionInput struct {
	RegionId uint `json:"regionId"`
}

var _ servicer.Servicer = new(RemoveRegionInput)

func (input *RemoveRegionInput) CheckParams(ctx *gin.Context) error {
	if input.RegionId <= 0 {
		return errors.New("invalid regionId")
	}
	return nil
}

func (input *RemoveRegionInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	clusterList, err := tbl_cluster.GetList(input.RegionId, 0, def.CLUSTER_STATUS_ONLINE)
	if err != nil {
		log.Warn("get cluster list failed.err:", err)
		return nil, err
	}
	for _, cluster := range clusterList {
		fail, err := mdl_cluster.OfflineClusterNodes(cluster.Id, cluster.ServiceId, nil, nil)
		if err != nil {
			log.Warn("offline cluster nodes failed.err:", err)
			return nil, err
		}
		if fail > 0 {
			return nil, errors.New("still got alive nodes in this region. Could not remove the region.")
		}
	}
	if err := tbl_regionmachine.DeleteByRegion(input.RegionId); err != nil {
		return nil, err
	}
	return nil, tbl_region.Delete(input.RegionId)
}
