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
	"github.com/zuoyebang/bitalostored/paas/dao/redis_cli"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_group"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_hostport"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/dao/web/dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_port"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_task"
	"github.com/zuoyebang/bitalostored/paas/model/redis_op"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"strconv"
	"time"
)

type GroupReraftInput struct {
	Token     string `json:"token"`
	Port      uint   `json:"port"`
	ClusterId uint   `json:"clusterId"`
	GroupId   uint   `json:"groupId"`
	NodeId    uint   `json:"nodeId"`
}

var _ servicer.Servicer = new(GroupReraftInput)

func (input *GroupReraftInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	if input.GroupId <= 0 {
		return errors.New("invalid groupId")
	}
	if input.NodeId <= 0 {
		return errors.New("invalid nodeId")
	}
	return nil
}

func (input *GroupReraftInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	token := input.Token
	newPort := input.Port
	clusterInfo, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil {
		log.Warnf("failed to get cluster info.err:%+v", err)
		return nil, err
	}
	if len(input.Token) <= 0 {
		token = math2.GetMd5(clusterInfo.Name)
	}

	node, err := tbl_node.GetInfo(input.NodeId, input.GroupId, input.ClusterId)
	if err != nil {
		log.Warnf("failed to get node info.err:%+v", err)
		return nil, err
	}
	if node.IsWitness {
		return nil, errors.New("witness forbid")
	}
	machine, err := tbl_machine.GetInfo(node.MachineId)
	if err != nil {
		log.Warnf("failed to get machine info.err:%+v", err)
		return nil, err
	}

	address := machine.IP + ":" + strconv.Itoa(int(node.ServicePort))
	single, err := redis_op.IsSingle(address, input.ClusterId, input.GroupId, clusterInfo.Name)
	if err != nil {
		return nil, err
	}
	if !single {
		return nil, errors.New("Only the nodes that have been rafted can be rafted again.")
	}

	serviceInfo, err := tbl_service.GetInfo(clusterInfo.ServiceId)
	if err != nil {
		log.Warnf("get mdl_service info failed.serviceId:%d", clusterInfo.ServiceId)
		return nil, err
	}
	portRange := mdl_port.NarrowDownPortRange(serviceInfo.ClusterPortRanges, machine.ID)
	if len(portRange) <= 0 {
		return nil, errors.New("narrowdown port empty")
	}
	if newPort <= 0 {
		newPort = uint(portRange[0])
	}
	portExist := tbl_hostport.IsExist(machine.ID, newPort)
	if portExist {
		return nil, errors.New("port exist")
	}
	hostRes, err := tbl_hostport.Create(machine.ID, newPort, machine.IP)
	if err != nil {
		return nil, errors.New("create port failed")
	}

	cli, err := redis_cli.NewClient(address, config.GetAuth(input.ClusterId, ""), 5*time.Second)
	if err != nil {
		log.Errorf("could not connect to redis.err:%+v", err)
		_ = tbl_hostport.DeleteById(hostRes.ID)
		return nil, err
	}
	if err := cli.Reraft(token, newPort); err != nil {
		log.Errorf("reraft failed,err:%+v", err)
		_ = tbl_hostport.DeleteById(hostRes.ID)
		return nil, err
	}
	if err = dashboard.SetDashboardCookie(ctx); err != nil {
		return nil, errors.New("failed to convert role")
	}
	changeErr := dashboard.ChangeServerRole(ctx, clusterInfo.Name, "master_slave_node", address, input.GroupId)
	if changeErr != nil {
		return nil, errors.New("change role failed")
	}
	raft := fmt.Sprintf("%s:%d", machine.IP, newPort)
	initRaftStr := fmt.Sprintf("[\"%s\"]", raft)
	initNodeId := mdl_task.FormatNodeId([]uint{input.NodeId})
	err = tbl_group.Update(input.ClusterId, input.GroupId, tbl_group.Group{
		InitRaft:   initRaftStr,
		InitNodeId: initNodeId,
	})
	if err != nil {
		log.Warn("update db raft failed")
		return nil, err
	}
	err = tbl_node.Update(input.NodeId, input.GroupId, input.ClusterId, tbl_node.Node{ClusterPort: newPort})
	if err != nil {
		log.Warn("update node cluster port failed")
		return nil, err
	}

	task := &tbl_task.Task{
		ServiceId: clusterInfo.ServiceId,
		GroupId:   input.GroupId,
		NodeId:    input.NodeId,
		ClusterId: input.ClusterId,
		TaskExt: tbl_task.TaskExtra{
			NodeIndex:   int(input.NodeId),
			ServicePort: node.ServicePort,
			HostName:    machine.HostName,
			DeraftToken: math2.GetMd5(clusterInfo.Name),
			IsObserver:  false,
			NodeListVal: raft,
			NodeListStr: initRaftStr,
			NodeIdList:  initNodeId,
		},
	}
	err = mdl_node.UpdateNodeConfig(clusterInfo.ConfigPackId, task)
	if err != nil {
		log.Warn("update config failed")
	}

	return nil, nil
}
