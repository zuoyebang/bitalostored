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

package srv_config

import (
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_config"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_config"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"gorm.io/gorm"
	"sort"
)

type ConfigListInput struct {
	ClusterId    uint   `form:"clusterId"`
	ConfigPackId uint   `form:"configPackId"`
	ServiceId    uint   `form:"serviceId"`
	ClusterName  string `form:"clusterName"`
}

var _ servicer.Servicer = new(ConfigListInput)

func (input *ConfigListInput) CheckParams(ctx *gin.Context) error {
	if input.ServiceId == 0 {
		return errors.New("invalid serviceId")
	}
	return nil
}
func (input *ConfigListInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	if len(input.ClusterName) > 0 {
		clusterName := input.ClusterName
		if clusterName == "ocr-search-page" && input.ServiceId == def.SERVICE_ID_BITALOS {
			clusterName = "our-search-page"
		}
		if clusterName == "ocr-search-inv" && input.ServiceId == def.SERVICE_ID_BITALOS {
			clusterName = "our-search-inv"
		}
		cluster, err := tbl_cluster.GetInfoByName(clusterName, input.ServiceId)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			serviceId := input.ServiceId
			if input.ServiceId == def.SERVICE_ID_BITALOS {
				serviceId = def.SERVICE_ID_MATRIX
			}
			cluster, err = tbl_cluster.GetInfoByName(clusterName, serviceId)
		}
		if err != nil {
			return nil, err
		}
		if cluster.Id > 0 {
			cluster, err := mdl_config.GetConfigsByCluster(cluster.Id, input.ServiceId)
			sort.SliceStable(cluster, func(i, j int) bool {
				return cluster[i].ServiceId > cluster[j].ServiceId
			})
			return cluster, err
		}
	}

	if input.ClusterId == 0 && input.ConfigPackId == 0 {
		pack, err := tbl_config.GetListByPack(1, input.ServiceId)
		sort.SliceStable(pack, func(i, j int) bool {
			return pack[i].ServiceId > pack[j].ServiceId
		})
		return pack, err
	}
	if input.ClusterId == 0 {
		pack, err := tbl_config.GetListByPack(input.ConfigPackId, input.ServiceId)
		sort.SliceStable(pack, func(i, j int) bool {
			return pack[i].ServiceId > pack[j].ServiceId
		})
		return pack, err
	}
	clusterConfig, err := mdl_config.GetConfigsByCluster(input.ClusterId, input.ServiceId)
	sort.SliceStable(clusterConfig, func(i, j int) bool {
		return clusterConfig[i].ServiceId > clusterConfig[j].ServiceId
	})
	return clusterConfig, err
}
