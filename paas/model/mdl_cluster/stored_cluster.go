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
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type NameList struct {
	List []*CascaderChildren `json:"list"`
}

type CascaderChildren struct {
	Value    string              `json:"value"`
	Label    string              `json:"label"`
	Children []*CascaderChildren `json:"children"`
}

func GetStoredCluster(storedId uint, serviceName string) (string, error) {
	storedCluster, err := tbl_cluster.SameStoredCluster(storedId)
	if err != nil {
		log.Error("get stored cluster failed.err:", err)
		return "", err
	}
	var clusterId uint
	for _, cluster := range storedCluster {
		if serviceInfo, err := tbl_service.GetInfo(cluster.ServiceId); err == nil {
			if serviceInfo.Name == serviceName {
				clusterId = cluster.Id
				break
			}
		}
	}
	if clusterId == 0 {
		return "", errors.New("need to pre install other service")
	}
	nodes, err := tbl_node.GetListByCluster(clusterId)
	if err != nil {
		log.Error("get cluster node failed.err:", err)
		return "", err
	}
	if len(nodes) != 1 {
		return "", errors.New("incorrect storedId or clusterId")
	}
	machine, err := tbl_machine.GetInfo(nodes[0].MachineId)
	if err != nil {
		log.Error("get machines failed.err:", err)
		return "", err
	}
	return fmt.Sprintf("%s:%d", machine.IP, nodes[0].ServicePort), nil
}

func GetDashboardName(storedId uint, serviceName string) string {
	storedCluster, err := tbl_cluster.SameStoredCluster(storedId)
	if err != nil {
		log.Error("get stored cluster failed.err:", err)
		return ""
	}
	var clusterInfo *tbl_cluster.Cluster
	for _, cluster := range storedCluster {
		if serviceInfo, err := tbl_service.GetInfo(cluster.ServiceId); err == nil {
			if serviceInfo.Name == serviceName {
				clusterInfo = cluster
				break
			}
		}
	}
	return clusterInfo.Name
}
