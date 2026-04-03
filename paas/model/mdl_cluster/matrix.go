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

package mdl_cluster

import (
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

func GetBasicInfo(serviceId, regionId, storedId, configPackId uint, clusterName string, department string) (*tbl_service.Service, *tbl_region.Region, *tbl_cluster.Cluster, error) {
	serviceInfo, err := tbl_service.GetInfo(serviceId)
	if err != nil {
		log.Warnf("get service info failed.serviceId:%d.err:%+v", serviceId, err)
		return nil, nil, nil, err
	}

	regionInfo, err := tbl_region.GetInfo(regionId)
	if err != nil {
		log.Warnf("get region info failed.regionId:%d.err:%+v", regionId, err)
		return nil, nil, nil, err
	}

	clusterInfo, err := tbl_cluster.Create(clusterName, regionId, serviceId, storedId, configPackId, department)
	if err != nil {
		log.Warnf("get cluster info failed.err:%+v", err)
		return nil, nil, nil, err
	}
	return serviceInfo, regionInfo, clusterInfo, nil
}
