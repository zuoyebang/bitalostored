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
	"github.com/zuoyebang/bitalostored/paas/dao/redis_cli"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_group"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/dao/web/dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/redis_op"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strconv"
	"time"

	"gorm.io/gorm"
)

type MdlNode struct {
	*tbl_node.Node
	ClusterName string `json:"clusterName"`
	ServiceName string `json:"serviceName"`
	IP          string `json:"ip"`
	IDC         string `json:"idc"`
	Locked      bool   `json:"locked"`
	Witness     bool   `json:"witness"`
	Replica     bool   `json:"replica"`
	Role        string `json:"role"`
}

func (n MdlNode) GetRedisClient() *redis_cli.Client {
	address := n.IP + ":" + strconv.Itoa(int(n.ServicePort))
	cli, err := redis_cli.NewClient(address, config.GetAuth(n.ClusterId, ""), 5*time.Second)
	if err != nil {
		return nil
	}
	return cli
}

// If any node is not connected, stop the copy process
func GetNodeListForCopy(clusterInfo *tbl_cluster.Cluster, groupId uint) ([]MdlNode, error) {
	var mn []MdlNode
	nodeList, err := tbl_node.GetListByGroup(clusterInfo.Id, groupId, -1, 0)
	if err != nil {
		log.Warnf("failed to get region clusters.err:%+v", err)
		return nil, err
	}

	// Make sure the version of all node is v7 or above
	// Exclude witness, observer, dead node, removed node (no cluster info or status is flase)
	for _, node := range nodeList {
		if node.IsWitness || node.Status != "online" {
			continue
		}
		if node.ServicePort <= 0 {
			continue
		}
		mInfo, _ := tbl_machine.GetInfo(node.MachineId)

		address := mInfo.IP + ":" + strconv.Itoa(int(node.ServicePort))
		cli, err := redis_cli.NewClient(address, config.GetAuth(clusterInfo.Id, ""), 5*time.Second)
		if err != nil {
			log.Errorf("could not connect to redis.err:%+v", err)
			return nil, err
		}

		infos, err := cli.MergeInfoV67(uint64(groupId))
		if err != nil {
			log.Errorf("get info failed.err:%+v", err)
			return nil, err
		}
		if _, ok := infos["major_version"]; !ok {
			return nil, fmt.Errorf("all nodes version must >= v7")
		}
		if v, ok := infos["status"]; !ok || v == "false" {
			log.Infof("node (status is false) is excluded. v:%s ok:%t node:%d addr:%s", v, ok, node.NodeId, address)
			continue
		}

		role := redis_op.GetRaftRole(infos)
		if role == "" || role == "observer" {
			continue
		}
		mn = append(mn, MdlNode{
			Node:        node,
			ClusterName: clusterInfo.Name,
			IP:          mInfo.IP,
			IDC:         mInfo.IDC,
			Role:        role,
		})
	}
	return mn, nil
}

func GetDataNodeList(clusterInfo *tbl_cluster.Cluster, groupId uint) ([]MdlNode, error) {
	var mn []MdlNode
	nodeList, err := tbl_node.GetListByGroup(clusterInfo.Id, groupId, -1, 0)
	if err != nil {
		log.Warnf("failed to get region clusters.err:%+v", err)
		return mn, err
	}

	for _, node := range nodeList {
		if node.Status != "online" {
			continue
		}

		mInfo, _ := tbl_machine.GetInfo(node.MachineId)
		mn = append(mn, MdlNode{
			Node:        node,
			ClusterName: clusterInfo.Name,
			IP:          mInfo.IP,
			IDC:         mInfo.IDC,
		})
	}
	return mn, nil
}

func GetNodeList(clusterInfo *tbl_cluster.Cluster, groupId, serviceId uint) ([]MdlNode, error) {
	var mn []MdlNode
	nodeList, err := tbl_node.GetListByGroup(clusterInfo.Id, groupId, -1, 0)
	if err != nil {
		log.Warnf("failed to get region clusters.err:%+v", err)
		return mn, err
	}
	locked := false
	groupInfo, err := tbl_group.GetInfo(clusterInfo.Id, groupId)
	if err == nil && groupInfo != nil {
		locked = groupInfo.Lock
	}
	serviceInfo, _ := tbl_service.GetInfo(serviceId)
	for _, node := range nodeList {
		mInfo, _ := tbl_machine.GetInfo(node.MachineId)
		role := "unknown"
		if node.ServicePort > 0 {
			address := mInfo.IP + ":" + strconv.Itoa(int(node.ServicePort))
			role, err = redis_op.GetNodeRole(address, clusterInfo.Id, groupId, clusterInfo.Name)
			if err != nil {
				log.Errorf("could not connect to redis.err:%+v", err)
			}
		}
		mn = append(mn, MdlNode{
			Node:        node,
			ClusterName: clusterInfo.Name,
			IP:          mInfo.IP,
			IDC:         mInfo.IDC,
			ServiceName: serviceInfo.Name,
			Locked:      locked,
			Witness:     node.IsWitness,
			Role:        role,
		})
	}
	return mn, nil
}

func GetMatrixNodeList(clusterInfo *tbl_cluster.Cluster, groupId, serviceId uint, locked bool, replicaMap map[string]bool) ([]MdlNode, error) {
	var mn []MdlNode
	nodeList, err := tbl_node.GetListByGroup(clusterInfo.Id, groupId, -1, 0)
	if err != nil {
		log.Warnf("failed to get region clusters.err:%+v", err)
		return mn, err
	}
	serviceInfo, _ := tbl_service.GetInfo(serviceId)
	for _, node := range nodeList {
		mInfo, _ := tbl_machine.GetInfo(node.MachineId)
		node.ConfigContent = ""
		role := "unknown"
		address := mInfo.IP + ":" + strconv.Itoa(int(node.ServicePort))
		if node.ServicePort > 0 {
			role, err = redis_op.GetNodeRole(address, clusterInfo.Id, groupId, clusterInfo.Name)
			if err != nil {
				log.Errorf("could not connect to redis.err:%+v", err)
			}
		}
		mn = append(mn, MdlNode{
			Node:        node,
			ClusterName: clusterInfo.Name,
			IP:          address,
			IDC:         mInfo.IDC,
			ServiceName: serviceInfo.Name,
			Locked:      locked,
			Replica:     replicaMap[address],
			Witness:     node.IsWitness,
			Role:        role,
		})
	}
	return mn, nil
}

func GetClusterList(cosFileId uint) ([]string, error) {
	clusterIdGroupByCosFileId, err := tbl_node.GetClusterIdGroupByCosFileId(cosFileId)
	if err != nil {
		return nil, err
	}
	if len(clusterIdGroupByCosFileId) <= 0 {
		return nil, nil
	}
	clusterIds := make([]uint, 0)
	for _, v := range clusterIdGroupByCosFileId {
		clusterIds = append(clusterIds, v.ClusterId)
	}
	ids, err := tbl_cluster.GetListByIds(clusterIds)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0)
	for _, v := range ids {
		res = append(res, v.Name)
	}
	return res, nil
}

func GetOneProxy(clusterName string, serviceId uint) (string, uint, error) {
	infos, err := tbl_cluster.GetInfoByName(clusterName, serviceId)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return "", 0, err
	}
	if infos.Id <= 0 {
		if clusterName == "our-search-inv" {
			infos, err = tbl_cluster.GetInfoByName("ocr-search-inv", serviceId)
			if err != nil {
				return "", 0, err
			}
			if infos.Id <= 0 {
				log.Warnf("clusterName:%s serviceId:%d", "ocr-search-inv", serviceId)
				return "", 0, errors.New("empty infos")
			}
		} else if clusterName == "our-search-page" {
			infos, err = tbl_cluster.GetInfoByName("ocr-search-page", serviceId)
			if err != nil {
				return "", 0, err
			}
			if infos.Id <= 0 {
				log.Warnf("clusterName:%s serviceId:%d", "ocr-search-page", serviceId)
				return "", 0, errors.New("empty infos")
			}
		} else {
			log.Warnf("clusterId:%d serviceId:%d", infos.Id, serviceId)
			return "", 0, errors.New("empty infos")
		}
	}
	nodes, err := tbl_node.GetOnlineClusterMachine(infos.Id, serviceId, false)
	if err != nil {
		return "", 0, err
	}
	if len(nodes) <= 0 {
		log.Warnf("clusterId:%d serviceId:%d", infos.Id, serviceId)
		return "", 0, errors.New("empty nodes")
	}
	machineInfo, err := tbl_machine.GetInfo(nodes[0].MachineId)
	if err != nil {
		return "", 0, err
	}
	if len(machineInfo.IP) <= 0 {
		return "", 0, errors.New("empty ip")
	}
	return machineInfo.IP, nodes[0].ClusterPort, nil
}

func GetSlotsGroupIds(clusterName string, serviceId uint) (map[uint]map[string]string, error) {
	groupIds := make(map[uint]map[string]string, 0)
	proxyIp, proxyClusterPort, err := GetOneProxy(clusterName, serviceId)
	if err != nil {
		return nil, err
	}
	proxyAddress := fmt.Sprintf("%s:%d", proxyIp, proxyClusterPort)
	proxySlots, err := dashboard.GetSlots(proxyAddress)
	if err != nil {
		return nil, err
	}
	if proxySlots != nil {
		for _, slot := range proxySlots.Data {
			groupIds[slot.MasterGroupId] = slot.GroupServers
		}
	}
	return groupIds, nil
}

func GetSlotMasters(clusterName string, serviceId uint) ([]*dashboard.SlotsData, error) {
	proxyIp, proxyClusterPort, err := GetOneProxy(clusterName, serviceId)
	if err != nil {
		return nil, err
	}
	proxyAddress := fmt.Sprintf("%s:%d", proxyIp, proxyClusterPort)
	proxySlots, err := dashboard.GetSlots(proxyAddress)
	if err != nil {
		return nil, err
	}
	return proxySlots.Data, nil
}
