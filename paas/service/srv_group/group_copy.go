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

package srv_group

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_group"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/dao/web/dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strconv"
	"strings"
	"time"
)

type CopyGroupInput struct {
	ClusterId uint `json:"clusterId"`
	GroupId   uint `json:"groupId"`
	CosFileId uint `json:"packageId"`
}

var _ servicer.Servicer = new(CopyGroupInput)

func (input *CopyGroupInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	if input.GroupId <= 0 {
		return errors.New("invalid GroupId")
	}
	if input.CosFileId <= 0 {
		return errors.New("invalid CosFileId")
	}
	return nil
}

func (input *CopyGroupInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	var err error
	clusterInfo, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil {
		log.Warn("get basic info failed.err:", err)
		return nil, err
	}

	clusterInfo.ServiceId = def.SERVICE_ID_BITALOS

	serviceInfo, err := tbl_service.GetInfo(def.SERVICE_ID_BITALOS)
	if err != nil {
		return nil, err
	}
	serviceName := serviceInfo.Name

	_, err = tbl_group.GetInfo(input.ClusterId, input.GroupId)
	if err != nil {
		log.Warn("get group failed.err:", err)
		return nil, err
	}
	nodeList, err := mdl_node.GetNodeListForCopy(clusterInfo, input.GroupId)
	if err != nil {
		log.Warn("get node failed.err:", err)
		return nil, err
	}
	if len(nodeList) <= 0 {
		return nil, fmt.Errorf("not available node")
	}
	if len(nodeList) == 1 {
		return nil, fmt.Errorf("only one v7 node")
	}

	targetGroup, err := tbl_group.Create(clusterInfo.Id, clusterInfo.ServiceId)
	if err != nil {
		log.Warn("create group failed.err:", err)
		return nil, err
	}

	initIdList := make([]string, 0, len(nodeList))
	initAddrList := make([]string, 0, len(nodeList))
	for _, nd := range nodeList {
		initIdList = append(initIdList, strconv.Itoa(int(nd.NodeId)))
		initAddrList = append(initAddrList, nd.IP+":"+strconv.Itoa(int(nd.ClusterPort)))
	}

	err = tbl_group.Update(input.ClusterId, targetGroup.GroupId, tbl_group.Group{
		InitRaft:   fmt.Sprintf("[\"%v\"]", strings.Join(initAddrList, "\",\"")),
		InitNodeId: fmt.Sprintf("[%s]", strings.Join(initIdList, ",")),
	})
	if err != nil {
		log.Warn("update group init raft info failed.err:", err)
		return nil, err
	}
	regionName, dashboardName, err := tbl_task.GetClusterTaskInfo(input.ClusterId)
	if err != nil {
		log.Warn("get task info err:", err)
		return nil, err
	}

	for _, nd := range nodeList {
		_, err := mdl_node.CopyNode(nd.Node, targetGroup.GroupId)
		if err != nil {
			log.Warn("create node failed.err:", err)
			return nil, err
		}
		task := &tbl_task.Task{
			Type:      def.TASK_TYPE_LINK,
			Status:    def.TASK_NEW,
			RegionId:  nd.RegionId,
			MachineId: nd.MachineId,
			ServiceId: nd.ServiceId,
			ClusterId: input.ClusterId,
			GroupId:   input.GroupId,
			NodeId:    nd.NodeId,
			TaskExt: tbl_task.TaskExtra{
				RegionName:    regionName,
				ClusterName:   clusterInfo.Name,
				ServiceName:   serviceName,
				DashboardName: dashboardName,
				TargetGroupId: targetGroup.GroupId,
				ServicePort:   nd.ServicePort,
			},
		}
		err = tbl_task.CreateTask(task)
		if err != nil {
			log.Errorf("create link task faild task:%+v err:%+v", *task, err)
			return nil, err
		}
	}
	// addcluster clusterId nodeId nodeAddrList nodeIdList
	// nodeAddrList: 1.1.1.1:10001,1.1.1.1:10002
	// nodeIdList: 1,2
	for _, nd := range nodeList {
		cli := nd.GetRedisClient()
		if cli != nil {
			_, err = cli.Do("addcluster", targetGroup.GroupId, nd.NodeId, strings.Join(initAddrList, ","), strings.Join(initIdList, ","))
			if err != nil {
				log.Warnf("addcluster gid:%d nid:%d addrs:%s ids:%s err:%v", targetGroup.GroupId, nd.NodeId, strings.Join(initAddrList, ","), strings.Join(initIdList, ","), err)
			}
		}
	}

	if err := dashboard.SetDashboardCookie(ctx); err != nil {
		log.Errorf("cookie set err:%s", err)
		return nil, err
	}
	err = dashboard.CreateNewGroup(ctx, dashboardName, int(targetGroup.GroupId))
	if err != nil {
		log.Errorf("create new group failed in dashboard.err:%+v.groupId:%d", err, targetGroup.GroupId)
		return nil, err
	}
	var machineInfos []dashboard.MachineInfo
	for _, nodeInfo := range nodeList {
		machineInfos = append(machineInfos, dashboard.MachineInfo{
			IpPort: nodeInfo.IP + ":" + strconv.Itoa(int(nodeInfo.ServicePort)),
			IDC:    nodeInfo.IDC,
		})
	}
	newGroupId := targetGroup.GroupId
	err = dashboard.AddNodesToGroup(ctx, machineInfos, dashboardName, int(newGroupId))
	if err != nil {
		log.Warnf("failed to add node to dashboard.err:%v", err)
		time.Sleep(time.Second * 3)
		err = dashboard.AddNodesToGroup(ctx, machineInfos, dashboardName, int(newGroupId))
		if err != nil {
			log.Warnf("failed to add node to dashboard.err:%v", err)
			return nil, err
		}
	}
	return nil, nil
}
