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

package srv_machine

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_machine"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/model/redis_op"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strconv"
)

type MachineRebalancedInput struct {
	InstancesFromMachines []uint `json:"instancesFromMachines"`
	InstancesToMachines   []uint `json:"instancesToMachines"`
	RegionId              uint   `json:"regionId"`
	ServiceId             uint   `json:"serviceId"`
}

var _ servicer.Servicer = new(MachineRebalancedInput)

func (input *MachineRebalancedInput) CheckParams(ctx *gin.Context) error {
	if input.RegionId <= 0 {
		return errors.New("invalid regionId")
	}
	if input.ServiceId <= 0 {
		return errors.New("invalid serviceId")
	}
	return nil
}
func (input *MachineRebalancedInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	var balanceContainer calContainer
	err := balanceContainer.initContainer(input.RegionId, input.ServiceId)
	if err != nil {
		return nil, err
	}

	if len(input.InstancesFromMachines) == 0 {

		for {
			burdenBeforeMigrate := balanceContainer.calAverageBurden()
			nodeInfo := balanceContainer.getMoveOutNodeInfo(input)
			if nodeInfo == nil {
				return nil, nil
			}
			err := balanceContainer.processNode(input, nodeInfo)
			if err != nil {
				log.Errorf("could not migrate nodes")
				return nil, err
			}
			burdenAfterMigrate := balanceContainer.calAverageBurden()
			if burdenAfterMigrate >= burdenBeforeMigrate {
				balanceContainer.rollbackNode()
				break
			}
		}
		return balanceContainer.dumpToDB(), nil
	}

	var outInstances []*tbl_node.Node
	for _, id := range input.InstancesFromMachines {
		machineNodes := getMachineNodes(id, input.ServiceId)
		if hasMasterNode(machineNodes) {
			return errors.New("master node included."), nil
		}
		outInstances = append(outInstances, machineNodes...)
	}
	for _, nodeInfo := range outInstances {
		err := balanceContainer.processNode(input, nodeInfo)
		if err != nil {
			log.Errorf("could not migrate nodes")
			return nil, err
		}
	}
	return nil, balanceContainer.dumpToDB()
}

func isMaster(nodeInfo *tbl_node.Node) bool {
	machineInfo, err := tbl_machine.GetInfo(nodeInfo.MachineId)
	if err != nil {
		log.Errorf("get machine info failed.nodeInfo:%+v.err:%+v", nodeInfo, err)
		return true
	}
	address := machineInfo.IP + ":" + strconv.Itoa(int(nodeInfo.ServicePort))
	return redis_op.MayBeNodeMaster(address, nodeInfo.ClusterId, nodeInfo.GroupId, "")
}

type calContainer struct {
	machineInfoList []*tbl_machine.Machine
	machineNodes    map[uint][]*tbl_node.Node
	taskList        []*tbl_task.Task
	newNodes        []*tbl_node.Node
}

func (c *calContainer) calAverageBurden() int {
	burden := 0
	sum := 0
	for _, machineInfo := range c.machineInfoList {
		weight := machineInfo.Weight
		burden = burden + c.calBurden(machineInfo.ID, weight)
		sum++
	}
	return burden / sum
}

func (c *calContainer) calBurden(machineId uint, weight int) int {
	if weight == 0 {
		log.Errorf("machineInfo incorrect.weight is 0.machineId:%d", machineId)
		return 0
	}
	return len(c.machineNodes[machineId])*def.BURDEN_MULTIPLIER + 10/weight
}

func (c *calContainer) processNode(input *MachineRebalancedInput, nodeInfo *tbl_node.Node) error {
	machineReceiver, err := c.getMoveInMachineId(input, nodeInfo)
	if err != nil {
		log.Errorf("could not get machine to migrate nodes.err:%+v", err)
		return err
	}
	newNodeInfo := c.newNode(machineReceiver, nodeInfo)
	c.appendMachineNode(newNodeInfo)
	err = c.newTask(newNodeInfo, nodeInfo)
	if err != nil {
		log.Errorf("create task error")
	}
	return nil
}

func (c *calContainer) rollbackNode() {
	if len(c.newNodes) == 0 {
		return
	}
	nodeInfo := c.newNodes[len(c.newNodes)-1]
	c.newNodes = c.newNodes[:len(c.newNodes)-1]
	c.taskList = c.taskList[:len(c.taskList)-1]
	c.machineNodes[nodeInfo.MachineId] = c.machineNodes[nodeInfo.MachineId][:len(c.machineNodes[nodeInfo.MachineId])-1]
}

func (c *calContainer) getMoveOutNodeInfo(input *MachineRebalancedInput) *tbl_node.Node {
	var migrateOutId uint
	for {
		migrateOutId = c.getMaxBurdenMachine(input.InstancesToMachines)
		if len(c.machineNodes[migrateOutId]) != 0 {
			nodeInfo := c.lendInstanceFromMachine(migrateOutId)
			if nodeInfo == nil {
				delete(c.machineNodes, migrateOutId)
				continue
			}
			return nodeInfo
		}
		delete(c.machineNodes, migrateOutId)
		if len(c.machineNodes) == 0 {
			log.Warn("empty nodes.")
			return nil
		}
	}
}

func (c *calContainer) getMoveInMachineId(input *MachineRebalancedInput, nodeInfo *tbl_node.Node) (uint, error) {
	if isMaster(nodeInfo) {
		return 0, errors.New("could not migrate master node")
	}
	machineInfo, err := tbl_machine.GetInfo(nodeInfo.MachineId)
	if err != nil {
		log.Errorf("get machine info failed.err:%+v.nodeInfo:%+v", err, nodeInfo)
		return 0, err
	}
	if !c.idcHasMachineToMoveIn(machineInfo.IDC, input.InstancesFromMachines) {
		log.Errorf("idc %s hasn't machine to move in instance.", machineInfo.IDC)
		return 0, errors.New(fmt.Sprintf("idc %s hasn't machine to move in instance", machineInfo.IDC))
	}
	machineReceiver := c.getMinBurdenMachine(machineInfo.IDC, input.InstancesFromMachines, input.InstancesToMachines)
	if machineReceiver == 0 {
		return 0, errors.New("could not find machine to receive instance")
	}
	machineId := machineReceiver
	skipMachineList := input.InstancesFromMachines
	for {
		if !c.hasSameGroupNode(machineId, nodeInfo.GroupId) {
			machineReceiver = machineId
			break
		}
		skipMachineList = append(skipMachineList, machineId)
		machineId = c.getMinBurdenMachine(machineInfo.IDC, skipMachineList, input.InstancesToMachines)
		if machineId == 0 {
			break
		}
	}
	return machineReceiver, nil
}

func (c *calContainer) initContainer(regionId, serviceId uint) error {
	c.machineNodes = make(map[uint][]*tbl_node.Node, 0)
	c.machineInfoList = mdl_machine.GetMachinesByRegion(regionId, []string{""})
	if c.machineInfoList == nil {
		return errors.New("failed to get region machines")
	}
	for _, machineInfo := range c.machineInfoList {
		instanceList := getMachineNodes(machineInfo.ID, serviceId)
		if len(instanceList) > 0 {
			c.machineNodes[machineInfo.ID] = instanceList
		}
	}
	return nil
}

func (c *calContainer) getMaxBurdenMachine(targetMachineList []uint) uint {
	var maxBurdenMachineId uint
	var burden int
	for _, machineInfo := range c.machineInfoList {
		if len(targetMachineList) != 0 && !isInMachineList(machineInfo.ID, targetMachineList) {
			continue
		}
		tempBurden := c.calBurden(machineInfo.ID, machineInfo.Weight)
		if burden == 0 && maxBurdenMachineId == 0 {
			burden = tempBurden
			maxBurdenMachineId = machineInfo.ID
			continue
		}
		if tempBurden > burden {
			burden = tempBurden
			maxBurdenMachineId = machineInfo.ID
			continue
		}
	}
	return maxBurdenMachineId
}

func (c *calContainer) idcHasMachineToMoveIn(idc string, skipMachineList []uint) bool {
	for _, machineInfo := range c.machineInfoList {
		if isInMachineList(machineInfo.ID, skipMachineList) {
			continue
		}
		if machineInfo.IDC == idc {
			return true
		}
	}
	return false
}

func (c *calContainer) getMinBurdenMachine(idc string, skipMachineList, targetMachineList []uint) uint {
	log.Infof("skipMachineList:%+v,targetMachineList%+v", skipMachineList, targetMachineList)
	var minBurdenMachineId uint
	var burden int
	log.Infof("calContainer:%+v", c)
	for _, machineInfo := range c.machineInfoList {
		if isInMachineList(machineInfo.ID, skipMachineList) {
			continue
		}
		if machineInfo.IDC != idc {
			continue
		}
		if len(targetMachineList) != 0 && !isInMachineList(machineInfo.ID, targetMachineList) {
			continue
		}
		tempBurden := c.calBurden(machineInfo.ID, machineInfo.Weight)

		if burden == 0 && minBurdenMachineId == 0 {
			burden = tempBurden
			minBurdenMachineId = machineInfo.ID
			continue
		}
		if tempBurden < burden {
			burden = tempBurden
			minBurdenMachineId = machineInfo.ID
			continue
		}
	}
	return minBurdenMachineId
}

func isInMachineList(machineId uint, machineList []uint) bool {
	for _, id := range machineList {
		if id == machineId {
			return true
		}
	}
	return false
}

func (c *calContainer) newNode(machineId uint, oldNode *tbl_node.Node) *tbl_node.Node {
	n := oldNode
	n.MachineId = machineId
	n.ServicePort = 0
	n.ClusterPort = 0
	n.Status = def.MACHINE_STATUS_OFFLINE
	n.CreateTime = 0
	n.UpdateTime = 0
	c.newNodes = append(c.newNodes, n)
	return n
}

func (c *calContainer) newTask(nodeInfo, oldNode *tbl_node.Node) error {
	machineInfo, err := tbl_machine.GetInfo(nodeInfo.MachineId)
	if err != nil {
		return err
	}
	//address := machineInfo.IP + ":" + strconv.Itoa(int(nodeInfo.ServicePort))

	regionInfo, err := tbl_region.GetInfo(nodeInfo.RegionId)
	if err != nil {
		return err
	}

	serviceInfo, err := tbl_service.GetInfo(nodeInfo.ServiceId)
	if err != nil {
		return err
	}

	clusterInfo, err := tbl_cluster.GetInfo(nodeInfo.ClusterId)
	if err != nil {
		return err
	}
	//if redis_op.IsNodeMaster(address, nodeInfo.ClusterId) {
	//	return errors.New("could not shutdown master node")
	//}

	taskType := def.TASK_TYPE_START
	if serviceInfo.Name == def.SERVICE_MATRIX {
		taskType = def.TASK_TYPE_PREPARESTART
	}
	newTask := tbl_task.Task{
		Type:      taskType,
		Status:    def.TASK_UNRELEASE,
		RegionId:  nodeInfo.RegionId,
		MachineId: nodeInfo.MachineId,
		ServiceId: nodeInfo.ServiceId,
		// PackageId: nodeInfo.PackageId,
		ClusterId: nodeInfo.ClusterId,
		GroupId:   nodeInfo.GroupId,
		TaskExt: tbl_task.TaskExtra{
			Ip:               machineInfo.IP,
			RegionName:       regionInfo.Name,
			ServiceName:      serviceInfo.Name,
			ServicePortRange: serviceInfo.PortRanges,
			ClusterPortRange: serviceInfo.ClusterPortRanges,
			ClusterName:      clusterInfo.Name,
			MigratedFromInfo: genMigrateInfo(oldNode),
		},
	}
	c.taskList = append(c.taskList, &newTask)
	return nil
}

func genMigrateInfo(oldNode *tbl_node.Node) string {
	migrateInfo := ""
	migrateInfo = fmt.Sprintf("oldNodeId:%d,", oldNode.NodeId)
	machineInfo, _ := tbl_machine.GetInfo(oldNode.MachineId)
	migrateInfo = migrateInfo + "IP:" + machineInfo.IP
	migrateInfo = fmt.Sprintf("%s,port:%d,clusterPort:%d", migrateInfo, oldNode.ServicePort, oldNode.ClusterPort)
	return migrateInfo
}

func (c *calContainer) dumpToDB() error {
	// if len(c.taskList) != len(c.newNodes) {
	// 	return errors.New("task num and node num is not equal.recalculate the node and task")
	// }
	// for index, nodeInfo := range c.newNodes {
	// 	pod, err := mdl_node.Create(nodeInfo.ClusterId, nodeInfo.GroupId, nodeInfo.RegionId, nodeInfo.MachineId, nodeInfo.ServiceId, nodeInfo.CosFileId)
	// 	if err != nil {
	// 		log.Errorf("create node failed.err:%+v", err)
	// 		return err
	// 	}
	// 	inTask := c.taskList[index]
	// 	t, err := mdl_task.Create(inTask.Type, inTask.RegionId, inTask.MachineId, inTask.ServiceId, "{}")
	// 	if err != nil {
	// 		log.Errorf("create task failed.err:%+v", err)
	// 		return err
	// 	}
	// 	time.Sleep(time.Millisecond * 50)
	// 	err = tbl_task.Update(t.ID, func(task *tbl_task.Task) bool {
	// 		task.ClusterId = inTask.ClusterId
	// 		task.GroupId = inTask.GroupId
	// 		task.NodeId = pod.NodeId
	// 		task.Status = inTask.Status

	// 		task.TaskExt.Ip = inTask.TaskExt.Ip
	// 		task.TaskExt.RegionName = inTask.TaskExt.RegionName
	// 		task.TaskExt.ServiceName = inTask.TaskExt.ServiceName
	// 		task.TaskExt.ServicePortRange = inTask.TaskExt.ServicePortRange
	// 		task.TaskExt.ClusterPortRange = inTask.TaskExt.ClusterPortRange
	// 		task.TaskExt.ClusterName = inTask.TaskExt.ClusterName
	// 		return true
	// 	})
	// 	if err != nil {
	// 		log.Errorf("update task failed.err:%+v", err)
	// 		return err
	// 	}
	// 	log.Infof("add task:%+v", t)
	// }
	return nil
}

func (c *calContainer) lendInstanceFromMachine(machineId uint) *tbl_node.Node {
	nodeList, ok := c.machineNodes[machineId]
	if !ok {
		return nil
	}
	for index, nodeInfo := range nodeList {
		if !isMaster(nodeInfo) {
			c.machineNodes[machineId] = append(c.machineNodes[machineId][:index], c.machineNodes[machineId][index+1:]...)
			return nodeInfo
		}
	}
	return nil
}

func (c *calContainer) appendMachineNode(n *tbl_node.Node) {
	c.machineNodes[n.NodeId] = append(c.machineNodes[n.NodeId], n)
}

func (c *calContainer) hasSameGroupNode(machineId, groupId uint) bool {
	for _, nodeInfo := range c.machineNodes[machineId] {
		if nodeInfo.GroupId == groupId {
			return true
		}
	}
	return false
}

func hasMasterNode(nodeInfos []*tbl_node.Node) bool {
	for _, info := range nodeInfos {
		if isMaster(info) {
			return true
		}
	}
	return false
}

func getMachineNodes(machineId, serviceId uint) []*tbl_node.Node {
	var instanceList []*tbl_node.Node
	nodeList, err := mdl_node.GetListByMachine(machineId, -1, 0)
	if err != nil {
		log.Errorf("get machine nodeList failed.machineId:%d.err:%+v", machineId, err)
		return instanceList
	}
	for _, nodeInfo := range nodeList {
		if nodeInfo.Status == def.NODE_STATUS_ONLINE && nodeInfo.ServiceId == serviceId {
			instanceList = append(instanceList, nodeInfo)
		}
	}
	log.Infof("machine nodes:%+v", instanceList)
	return instanceList
}
