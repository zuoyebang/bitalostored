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

package tbl_node

import (
	"errors"
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"sync"
	"time"

	"gorm.io/gorm"
)

const TableName = "tblNode"

type Node struct {
	ClusterId uint   `gorm:"column:cluster_id" json:"clusterId"`
	GroupId   uint   `gorm:"column:group_id" json:"groupId"`
	NodeId    uint   `gorm:"column:node_id" json:"nodeId"`
	Status    string `gorm:"column:status" json:"nodeStatus"`

	RegionId  uint `gorm:"column:region_id" json:"regionId"`
	MachineId uint `gorm:"column:machine_id" json:"machineId"`
	ServiceId uint `gorm:"column:service_id" json:"serviceId"`

	IsWitness      bool   `gorm:"column:is_witness" json:"isWitness"`
	CosFileId      uint   `gorm:"column:cos_file_id" json:"cosFileId"`
	CosFileVersion string `gorm:"column:cos_file_version" json:"version"`
	ConfigContent  string `gorm:"column:config_content" json:"configContent"`

	ServicePort uint `gorm:"column:service_port" json:"servicePort"` // redis port
	ClusterPort uint `gorm:"column:cluster_port" json:"clusterPort"` // raft port

	CreateTime int64  `gorm:"column:create_time" json:"-"`
	UpdateTime int64  `gorm:"column:update_time" json:"-"`
	CreateDate string `gorm:"-" json:"createTime"`
	UpdateDate string `gorm:"-" json:"updateTime"`
}

type OnlyCount struct {
	Total int `gorm:"total" json:"total"`
}

func Create(nodeInfo *Node, maxNodeId uint) (*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	uniqueIDLock.Lock()
	defer uniqueIDLock.Unlock()
	var nodeId uint
	if maxNodeId > 0 {
		nodeId = maxNodeId + 1
	} else {
		count, err := GetClusterGroupNodeCount(nodeInfo.ClusterId, nodeInfo.GroupId)
		if err != nil {
			if err.Error() == "record not found" {
				count = 0
			} else {
				log.Warn("err:", err)
				return nil, err
			}
		}
		nodeId = count + 1
	}
	currentTime := time.Now().Unix()
	nodeInfo.Status = def.NODE_STATUS_NEW
	nodeInfo.NodeId = nodeId
	nodeInfo.CreateTime = currentTime
	nodeInfo.UpdateTime = currentTime
	db.Create(nodeInfo)
	return nodeInfo, db.Error
}

var uniqueIDLock sync.Mutex

func getList(whereClause string, id uint, limit int, offset int) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where(whereClause, id)
	if limit >= 0 {
		db = db.Limit(limit).Offset(offset)
	}

	res := []*Node{}
	db.Find(&res)
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return res, db.Error
}

func GetOnlineListByGroup(clusterId, groupId uint, limit int, offset int) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("group_id = ? and cluster_id = ? and status = ?", groupId, clusterId, def.NODE_STATUS_ONLINE)
	if limit >= 0 {
		db = db.Limit(limit).Offset(offset)
	}

	var res []*Node
	db = db.Find(&res)
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return res, db.Error
}

func GetListByGroup(clusterId, groupId uint, limit int, offset int) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("group_id = ? and cluster_id = ?", groupId, clusterId)
	if limit >= 0 {
		db = db.Limit(limit).Offset(offset)
	}

	res := []*Node{}
	db.Find(&res)
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return res, db.Error
}

func GetOnlineNodesByGroup(clusterId, groupId uint) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("group_id = ? and cluster_id = ? and status = ?", groupId, clusterId, def.NODE_STATUS_ONLINE)

	res := []*Node{}
	db.Find(&res)
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return res, db.Error
}

func GetSimpleOnlineNodesByGroup(clusterId, groupId uint) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	db = db.Select("cluster_id, config_content, group_id, node_id, status, region_id, machine_id, service_id, is_witness, service_port, cluster_port, create_time, update_time").Where("group_id = ? and cluster_id = ? and status = ?", groupId, clusterId, def.NODE_STATUS_ONLINE)

	res := []*Node{}
	db.Find(&res)
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return res, db.Error
}

func CheckMachineDuplicatedByGroup(clusterId, groupId, machineId uint) (bool, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return false, err
	}
	db = db.Where("group_id = ? and cluster_id = ? and status = ? and machine_id = ?", groupId, clusterId, def.NODE_STATUS_ONLINE, machineId)
	res := []*Node{}
	db.Find(&res)
	return len(res) > 0, db.Error
}

func GetNodeByClusterPort(clusterId, groupId, clusterPort, machineId uint) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("group_id = ? and cluster_id = ? and machine_id = ? and cluster_port= ?", groupId, clusterId, machineId, clusterPort)
	res := []*Node{}
	db.Find(&res)
	return res, db.Error
}

func GetNodeByServicePort(clusterId, groupId, servicePort, machineId uint) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("group_id = ? and cluster_id = ? and machine_id = ? and service_port= ?", groupId, clusterId, machineId, servicePort)
	res := []*Node{}
	db.Find(&res)
	return res, db.Error
}

func GetListByCluster(clusterId uint) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("cluster_id = ?", clusterId)

	var res []*Node
	db.Find(&res)
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	if db.Error != nil {
		log.Errorf("query %s %d failed: [%v]", TableName, clusterId, db.Error)
	}
	return res, db.Error
}

func GetOneByClusterId(clusterId uint) (*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("cluster_id = ?", clusterId)

	var res *Node
	db.First(&res)
	res.CreateDate = math2.UnixTimeToStr(res.CreateTime)
	res.UpdateDate = math2.UnixTimeToStr(res.UpdateTime)
	return res, db.Error
}

func GetOnlineListByCluster(clusterId uint) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("cluster_id = ? and status = ?", clusterId, def.CLUSTER_STATUS_ONLINE)

	res := []*Node{}
	db.Find(&res)
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return res, db.Error
}

func GetOnlineListByClusterIds(clusterIds []uint) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("cluster_id in (?) and status = ?", clusterIds, def.CLUSTER_STATUS_ONLINE)

	var res []*Node
	db.Find(&res)
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return res, db.Error
}

func GetListByMachine(machineId uint, limit int, offset int) ([]*Node, error) {
	return getList("machine_id = ?", machineId, limit, offset)
}

func GetListByMachineRegion(machineId []uint, regionId uint) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	var res []*Node
	db = db.Where("machine_id in (?) and region_id = ?", machineId, regionId).Find(&res)
	return res, db.Error
}

func GetMachineOnlineNodes(machineId, serviceId uint, isWitness bool) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	res := []*Node{}
	db = db.Where("machine_id = ? and status = ? and service_id = ? and is_witness = ?", machineId, def.NODE_STATUS_ONLINE, serviceId, isWitness).Find(&res)
	return res, db.Error
}

func CountMachineNode(machineId, serviceId uint, isWitness bool) (int, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return 0, err
	}

	var nodeCount OnlyCount
	db = db.Select("count(1) as total").Where("machine_id = ? and status = ? and service_id = ? and is_witness = ?", machineId, def.NODE_STATUS_ONLINE, serviceId, isWitness).Find(&nodeCount)
	return nodeCount.Total, db.Error
}

func GetMachineOnlineNodeCount(machineId uint, serviceIds []uint) ([]*MachineClusterStat, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	var res []*MachineClusterStat
	db = db.Select("cluster_id, count(*) as total").Where("machine_id = ? and status = ? and service_id IN (?) and is_witness = ?", machineId, def.NODE_STATUS_ONLINE, serviceIds, def.NODE_NOT_WITNESS).Group("cluster_id").Find(&res)
	return res, db.Error
}

func GetMachinesOnlineNodes(machineIdList []uint, serviceId uint, isWitness bool) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	var res []*Node
	db = db.Where("machine_id in (?) and status = ? and service_id = ? and is_witness = ?", machineIdList, def.NODE_STATUS_ONLINE, serviceId, isWitness).Find(&res)
	return res, db.Error
}

func GetMachineAllNodes(machineId uint, serviceIds []uint, isWitness int) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	var res []*Node
	db = db.Where("machine_id = ? and status = ? and service_id in (?) and is_witness = ?", machineId, def.NODE_STATUS_ONLINE, serviceIds, isWitness).Find(&res)
	return res, db.Error
}

func GetMachineOnlineClusterNodes(machineId, clusterId uint, isWitness bool) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	var res []*Node
	db = db.Where("machine_id = ? and status = ? and cluster_id = ? and is_witness = ?", machineId, def.NODE_STATUS_ONLINE, clusterId, isWitness).Find(&res)
	return res, db.Error
}

func GetMachineNodes(machineId, serviceId uint) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	var res []*Node
	db = db.Where("machine_id = ? and status = ? and service_id = ?", machineId, def.NODE_STATUS_ONLINE, serviceId).Find(&res)
	return res, db.Error
}

func GetOnlineClusterMachine(clusterId, serviceId uint, isWitness bool) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	res := []*Node{}
	db = db.Where("cluster_id = ? and service_id = ? and is_witness = ? and status = ?", clusterId, serviceId, isWitness, def.NODE_STATUS_ONLINE).Find(&res)
	return res, db.Error
}

func GetServiceOnlineNodes(serviceId uint) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	res := []*Node{}
	db = db.Where("status = ? and service_id = ?", def.NODE_STATUS_ONLINE, serviceId).Find(&res)
	return res, db.Error
}

func DeleteByCluster(clusterId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("cluster_id = ?", clusterId).Delete(&Node{})

	return db.Error
}

func DeleteByGroup(clusterId, groupId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("cluster_id = ? and group_id = ?", clusterId, groupId).Delete(&Node{})

	return db.Error
}

func DeleteClusterOfflineNode(clusterId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("cluster_id = ? and status = ?", clusterId, def.NODE_STATUS_OFFLINE).Delete(&Node{})
	return db.Error
}

func DeleteNode(nodeId, groupId, clusterId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("cluster_id = ? and node_id = ? and group_id = ?", clusterId, nodeId, groupId).Delete(&Node{})
	return db.Error
}

func DeleteByMachine(machineId uint) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Where("machine_id = ?", machineId).Delete(&Node{})

	return db.Error
}

func GetWitnessListByCluster(clusterId uint) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("cluster_id = ? and status in (?)", clusterId,
		[]string{def.NODE_STATUS_NEW, def.NODE_STATUS_ONLINE})

	res := []*Node{}
	db.Find(&res)
	for _, r := range res {
		r.CreateDate = math2.UnixTimeToStr(r.CreateTime)
		r.UpdateDate = math2.UnixTimeToStr(r.UpdateTime)
	}
	return res, db.Error
}
func GetNodesByStatus(status string, clusterId, groupId uint) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	if status != "" {
		db = db.Where("cluster_id = ? and group_id = ? and status = ?", clusterId, groupId, status)
	} else {
		db = db.Where("cluster_id = ? and group_id = ? ", clusterId, groupId)
	}

	var res []*Node
	db.Find(&res)
	return res, db.Error
}

func GetProxyNodeByMachine(clusterId uint, machineId uint) (*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	db = db.Where("cluster_id = ? and service_id = ? and machine_id = ?", clusterId, def.SERVICE_ID_PROXY, machineId)
	res := &Node{}
	db.First(res)
	return res, db.Error
}

func GetInfo(nodeId, groupId, clusterId uint) (*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("node_id = ? and group_id = ? and cluster_id = ?", nodeId, groupId, clusterId)

	res := &Node{}
	db.First(res)
	return res, db.Error
}

func Copy(srcNode *Node, groupId uint) (*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}

	res := &Node{
		ClusterId: srcNode.ClusterId,
		GroupId:   groupId,
		Status:    def.NODE_STATUS_ONLINE,
		NodeId:    srcNode.NodeId,

		RegionId:      srcNode.RegionId,
		MachineId:     srcNode.MachineId,
		ServiceId:     srcNode.ServiceId,
		ConfigContent: srcNode.ConfigContent,

		ServicePort: srcNode.ServicePort,
		ClusterPort: srcNode.ClusterPort,

		CosFileId:      srcNode.CosFileId,
		CosFileVersion: srcNode.CosFileVersion,
		CreateTime:     time.Now().Unix(),
		UpdateTime:     time.Now().Unix(),
	}
	db.Create(res)
	return res, db.Error
}

func Update(nodeId, groupId, clusterId uint, node Node) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}

	node.UpdateTime = time.Now().Unix()
	db = db.Where("node_id = ? and group_id = ? and cluster_id = ?", nodeId, groupId, clusterId).UpdateColumns(node)
	if db.Error != nil {
		log.Errorf("update tbl_node failed, nodeId:%d groupId:%d clusterId:%d err:%d", nodeId, groupId, clusterId, db.Error)
	}
	return db.Error
}

func UpdateSql(nodeId, groupId, clusterId uint, node Node) (string, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return "", err
	}

	node.UpdateTime = time.Now().Unix()
	stmt := db.Session(&gorm.Session{DryRun: true}).Where("node_id = ? and group_id = ? and cluster_id = ?", nodeId, groupId, clusterId).UpdateColumns(node).Statement
	finalSQL := db.Dialector.Explain(stmt.SQL.String(), stmt.Vars...)
	return finalSQL, nil
}

func GetMachineServiceCount(machineId, serviceId uint, statusList []string) (int, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return -1, err
	}
	var nodeList []*Node
	err = db.Where("machine_id = ? and service_id = ? and status in (?)", machineId, serviceId, statusList).Find(&nodeList).Error
	if err != nil {
		return -1, err
	}
	return len(nodeList), nil
}

func GetClusterMachineCount(machineId, serviceId, clusterId uint, statusList []string) (int, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return -1, err
	}
	var nodeList []*Node
	err = db.Where("machine_id = ? and service_id = ? and cluster_id = ? and status in (?)", machineId, serviceId, clusterId, statusList).Find(&nodeList).Error
	if err != nil {
		return -1, err
	}
	return len(nodeList), nil
}

func GetClusterMachineCountByRole(machineId, serviceId, clusterId uint, statusList []string, isWitness bool) (int, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return -1, err
	}
	var nodeList []*Node
	err = db.Where("machine_id = ? and service_id = ? and cluster_id = ? and status in (?) and is_witness = ?", machineId, serviceId, clusterId, statusList, isWitness).Find(&nodeList).Error
	if err != nil {
		return -1, err
	}
	return len(nodeList), nil
}

func DeleteLittleNodes(clusterId, groupId uint) (uint, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return 0, err
	}
	db = db.Where("group_id = ? and cluster_id = ?", groupId, clusterId)
	res := Node{}
	db = db.Order("node_id desc").First(&res)
	if db.Error != nil {
		return 0, db.Error
	}
	db = db.Exec("delete from tblNode where group_id = ? and cluster_id = ? and status = ? and node_id < ?", groupId, clusterId, def.NODE_STATUS_OFFLINE, res.NodeId)
	return res.NodeId, db.Error
}

func GetClusterGroupNodeCount(clusterId, groupId uint) (uint, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return 0, err
	}
	db = db.Where("group_id = ? and cluster_id = ?", groupId, clusterId)
	res := Node{}
	db = db.Order("node_id desc").First(&res)
	return res.NodeId, db.Error
}

type ClusterIdGroupBy struct {
	ClusterId uint `gorm:"column:cluster_id" json:"clusterId"`
}

func GetClusterIdGroupByCosFileId(cosFileId uint) ([]ClusterIdGroupBy, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	res := make([]ClusterIdGroupBy, 0)
	err = db.Select("cluster_id").Where("cos_file_id = ? and status = ?", cosFileId, def.NODE_STATUS_ONLINE).Group("cluster_id").Find(&res).Error
	return res, err
}

type NodeStat struct {
	Total int `gorm:"column:total"`
}

func StatCluster(clusterId uint, machineIds []uint, includeWitness bool) (interface{}, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	db = db.Where("cluster_id = ?", clusterId)
	if len(machineIds) > 0 {
		db = db.Where("machine_id IN (?)", machineIds)
	}
	db = db.Where("status = ?", def.NODE_STATUS_ONLINE)

	if !includeWitness {
		db = db.Where("is_witness = ?", def.NODE_NOT_WITNESS)
	}

	db = db.Select("count(1) as total")
	res := NodeStat{}
	db = db.Find(&res).Limit(1)
	if errors.Is(db.Error, gorm.ErrRecordNotFound) {
		return res, nil
	}
	if db.Error != nil {
		return nil, db.Error
	}
	return res, nil
}

type MachineNodeStat struct {
	MachineId uint `gorm:"column:machine_id"`
	Total     uint `gorm:"column:total"`
}

func ListNodeForStat(clusterId uint, machineIds []uint, includeWitness bool) (interface{}, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	if len(machineIds) <= 0 {
		return nil, nil
	}

	db = db.Where("cluster_id = ?", clusterId)
	db = db.Where("machine_id IN (?)", machineIds)
	db = db.Where("status = ?", def.NODE_STATUS_ONLINE)

	if !includeWitness {
		db = db.Where("is_witness = ?", def.NODE_NOT_WITNESS)
	}

	db = db.Select("machine_id, count(*) as total")
	db = db.Group("machine_id")
	var res []*MachineNodeStat
	db = db.Find(&res)
	if db.Error != nil {
		return nil, db.Error
	}
	return res, nil
}

type MachineClusterStat struct {
	ClusterId uint `gorm:"column:cluster_id"`
	Total     uint `gorm:"column:total"`
}

func GetOnlineNodes(machineId uint) ([]*Node, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return nil, err
	}
	if machineId <= 0 {
		return nil, nil
	}
	db = db.Where("status = 'online' and machine_id = ?", machineId)
	var res []*Node
	db.Find(&res)
	return res, db.Error
}
