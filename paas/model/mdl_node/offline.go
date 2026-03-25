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

package mdl_node

import (
	"errors"
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/dao/web/dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/redis_op"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strconv"
)

func CheckDashboardAlive(node *tbl_node.Node) (bool, error) {
	machineInfo, err := tbl_machine.GetInfo(node.MachineId)
	if err != nil {
		return false, err
	}
	addr := machineInfo.IP + ":" + strconv.Itoa(int(node.ServicePort))
	return dashboard.IsDashboardNodeAlive(addr), nil
}

func UpdateOfflineNode(node *tbl_node.Node) (int, error) {
	mInfo, err := tbl_machine.GetInfo(node.MachineId)
	if err != nil || mInfo == nil {
		log.Warn("get node machine failed.err:", err)
		return 0, err
	}
	if node.Status == def.NODE_STATUS_OFFLINE {
		return 2, nil
	}
	sInfo, err := tbl_service.GetInfo(node.ServiceId)
	if err != nil {
		log.Warn("get node service failed.err:", err)
		return 0, err
	}
	if sInfo.Name == def.SERVICE_STORED_DASHBOARD {
		cInfo, _ := tbl_cluster.GetInfo(node.ClusterId)
		if dashboard.IsDashboardAlive(cInfo.Name) {
			return 0, nil
		}
		return 0, errors.New("could not update dashboard node status.unsupported")
	}
	if sInfo.Name == def.SERVICE_STORED_FE {
		return 0, errors.New("could not update fe node status.unsupported")
	}
	if !redis_op.IsOnline(mInfo.IP+":"+strconv.Itoa(int(node.ServicePort)), node.ClusterId) {
		log.Info("host:", mInfo.IP+":"+strconv.Itoa(int(node.ServicePort)), " is offline.nodeInfo:", node)
		err = tbl_node.Update(node.NodeId, node.GroupId, node.ClusterId, tbl_node.Node{Status: def.NODE_STATUS_OFFLINE})
		if err != nil {
			log.Warn("update node status failed.err:", err)
			return 0, err
		} else {
			return 1, nil
		}
	}
	return 0, nil
}

func MarkServerNodeOffline(nodeAddr string, nodeId, groupId, clusterId uint) error {
	checkAlive := redis_op.CheckOnlineRepeatly(nodeAddr, clusterId)
	if !checkAlive {
		log.Infof("group node offline. node:%s", nodeAddr)
		tbl_node.Update(nodeId, groupId, clusterId, tbl_node.Node{Status: def.NODE_STATUS_OFFLINE})
		return nil
	}

	var err error
	checkAlive, err = redis_op.CheckRaftStatus(nodeAddr, clusterId, groupId)
	log.Infof("group markoffline. node:%s alive:%t", nodeAddr, checkAlive)
	if err == nil && !checkAlive {
		log.Infof("group node offline. node:%s", nodeAddr)
		tbl_node.Update(nodeId, groupId, clusterId, tbl_node.Node{Status: def.NODE_STATUS_OFFLINE})
		return nil
	}
	return fmt.Errorf("node is online")
}
