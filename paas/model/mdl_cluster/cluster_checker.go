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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/model/redis_op"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strconv"
)

func OfflineClusterNodes(clusterId, serviceId uint, dashboardReplicas map[string]bool, proxyReplicaGroupIds map[uint]map[string]string) (int, error) {
	var fail int
	nodeList, err := tbl_node.GetOnlineListByCluster(clusterId)
	if err != nil {
		log.Warn("get node list failed.err:", err)
		return fail, err
	}

	for _, node := range nodeList {
		mInfo, err := tbl_machine.GetInfo(node.MachineId)
		if err != nil || mInfo == nil {
			log.Warnf("get node machine failed.machineId: %d err:%v", node.MachineId, err)
			continue
		}

		var checkAlive bool
		addr := mInfo.IP + ":" + strconv.Itoa(int(node.ServicePort))
		if replica, ok := dashboardReplicas[addr]; ok {
			if replica {
				log.Info("host:", addr, " is replica")
				continue
			}
		}
		if groupServers, ok := proxyReplicaGroupIds[node.GroupId]; ok {
			if _, ok := groupServers[addr]; ok {
				log.Infof("addr:%s has slots", addr)
				continue
			}
		}
		checkAlive = redis_op.CheckOnlineRepeatly(addr, clusterId)
		if !checkAlive {
			log.Info("host:", addr, " is offline.nodeInfo:", node)
			err = tbl_node.Update(node.NodeId, node.GroupId, node.ClusterId, tbl_node.Node{Status: def.NODE_STATUS_OFFLINE})
			if err != nil {
				fail++
				continue
			}
		}
	}
	return fail, nil
}
