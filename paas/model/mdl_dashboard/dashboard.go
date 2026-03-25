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

package mdl_dashboard

import (
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

func IsStoredSameName(storedId uint, clusterName string) error {
	storedClusters, err := tbl_cluster.SameStoredCluster(storedId)
	if err != nil {
		log.Error("get stored cluster failed.err:", err)
		return err
	}
	for _, cluster := range storedClusters {
		if serviceInfo, err := tbl_service.GetInfo(cluster.ServiceId); err == nil {
			if serviceInfo.Name == def.SERVICE_STORED_DASHBOARD {
				if cluster.Name != clusterName {
					return errors.New("should has same clusterName with it's dashboard")
				}
			}
		}
	}
	return nil
}

func GetDashboardName(storedId uint) (string, error) {
	var name string
	clusterInfos, err := tbl_cluster.SameStoredCluster(storedId)
	if err != nil {
		log.Error("get SameStructureCluster info failed.err:", err)
		return name, err
	}
	matrixName := ""
	bitalosName := ""
	for _, clusterInfo := range clusterInfos {
		serviceInfo, err := tbl_service.GetInfo(clusterInfo.ServiceId)
		if err != nil {
			log.Error("get service info failed.err:", err)
			return name, err
		}
		if serviceInfo.Name == def.SERVICE_STORED_DASHBOARD {
			name = clusterInfo.Name
			break
		}
		if serviceInfo.Name == def.SERVICE_MATRIX {
			matrixName = clusterInfo.Name
		}
		if serviceInfo.Name == def.SERVICE_BITALOS {
			bitalosName = clusterInfo.Name
		}
	}
	if len(name) <= 0 {
		if len(bitalosName) > 0 {
			name = bitalosName
		} else if len(matrixName) > 0 {
			name = matrixName
		}
	}
	if name == "our-search-page" {
		name = "ocr-search-page"
	}
	if name == "our-search-inv" {
		name = "ocr-search-inv"
	}
	return name, nil
}
