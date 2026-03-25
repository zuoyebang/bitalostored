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

package mdl_region

import (
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
)

func GetRegionClusters(regionId uint) ([]string, []string) {
	names := func(regionId, serviceId uint) []string {
		clusters, err := tbl_cluster.GetListByRegion(regionId, serviceId)
		if err != nil {
			return []string{}
		}
		var name []string
		for _, cluster := range clusters {
			name = append(name, cluster.Name)
		}
		return name
	}
	matrixService, _ := tbl_service.GetInfoByName(def.SERVICE_MATRIX)
	proxyService, _ := tbl_service.GetInfoByName(def.SERVICE_STORED_PROXY)
	return names(regionId, matrixService.ID), names(regionId, proxyService.ID)
}
