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

package mdl_group

import (
	"errors"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_group"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/model/redis_op"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strings"
	"time"
)

func GetGroupOnlinesNodes(clusterId uint) (map[uint][]*tbl_node.Node, error) {
	groupList, err := tbl_group.GetList(clusterId, def.GROUP_STATUS_ONLINE, -1, 0)
	if err != nil {
		return nil, err
	}
	res := make(map[uint][]*tbl_node.Node)
	for _, g := range groupList {
		nodeList, err := tbl_node.GetOnlineNodesByGroup(clusterId, g.GroupId)
		if err != nil {
			return nil, err
		}
		if len(nodeList) == 0 {
			continue
		}
		res[g.GroupId] = nodeList
	}
	return res, nil
}

func GetGroupInfo(clusterId, groupId, serviceId uint) ([]string, *tbl_group.Group, error) {
	var initRaft []string
	var groupInfo *tbl_group.Group
	var err error
	if !def.IsServer(serviceId) {
		return initRaft, groupInfo, nil
	}
	groupInfo, err = tbl_group.GetInfo(clusterId, groupId)
	if err != nil {
		log.Warn("get group info failed.err:", err)
		return nil, nil, err
	}
	if len(groupInfo.InitRaft) <= 0 || len(groupInfo.InitNodeId) <= 0 {
		return nil, nil, errors.New("initial raft info is missing")
	}
	trimleft := strings.TrimLeft(groupInfo.InitRaft, "[")
	trimright := strings.TrimRight(trimleft, "]")
	for _, s := range strings.Split(trimright, ",") {
		initRaft = append(initRaft, strings.Trim(s, "\""))
	}
	return initRaft, groupInfo, nil
}

func RoutineUpgradeRedis(address, clusterName string, clusterId, groupId uint) {
	timeStart := time.Now().Unix()
	log.Info("begin to restart redis.time:", timeStart)
	unlock := false
	defer func() {
		log.Infof("address: %v lock status:%v", address, unlock)
		if !unlock {
			tbl_group.LockGroup(clusterId, groupId, false)
		}
	}()
	for {
		if time.Now().Unix()-timeStart > 60*3 {
			log.Errorf("upgrade timeout")
			return
		}
		if redis_op.UpgradeRedis(address, clusterName, clusterId) == nil {
			unlock = tbl_group.LockGroup(clusterId, groupId, false)
			return
		}
		time.Sleep(time.Second * 10)
	}
}
