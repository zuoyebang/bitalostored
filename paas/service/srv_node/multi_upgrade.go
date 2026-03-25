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

package srv_node

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cosfile"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"sync"
)

type MultiUpgradeNodeInput struct {
	ClusterId      uint         `json:"clusterId"`
	GroupNodes     []*GroupNode `json:"groupNodes"`
	CosFileId      uint         `json:"packageId"`
	CosFileVersion string       `json:"version"`
	Operation      string       `json:"operation"`
	UpdateConfig   string       `json:"updateConfig"`
}

type GroupNode struct {
	GroupId uint   `json:"key"`
	NodeIds string `json:"value"`
}

var _ servicer.Servicer = new(MultiUpgradeNodeInput)

func (input *MultiUpgradeNodeInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	if input.GroupNodes == nil {
		return errors.New("invalid nodes")
	}
	//operation= upgrade  supervisor-stop  supervisor-start
	if input.Operation == "" {
		return errors.New("invalid operation")
	}
	if input.CosFileId <= 0 {
		return errors.New("invalid packageId")
	}
	return nil
}

type NodeMap struct {
	sync.Mutex
	Nodes map[uint]map[string]uint
}

func (input *MultiUpgradeNodeInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	clusterInfo, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil {
		log.Warnf("get cluster fail err:%v", err)
		return nil, err
	}
	dashboardName, err := mdl_dashboard.GetDashboardName(clusterInfo.StoredId)
	if err != nil {
		log.Warn("get dashboard name failed.err:", err)
		dashboardName = clusterInfo.Name
	}
	replicas, err := GetReplica(dashboardName)
	if err != nil {
		log.Warn("get replica failed.err:", err)
		return nil, err
	}
	cosFile, err := tbl_cosfile.GetCosFile(input.CosFileId)
	if err != nil {
		return nil, errors.New("get cos file failed")
	}
	input.CosFileVersion = cosFile.Version
	nodeMap := &NodeMap{
		Nodes: make(map[uint]map[string]uint),
	}
	if input.Operation == def.OPERATION_BITALOS_UPGRADE {
		gids := make([]uint, 0)
		for _, gn := range input.GroupNodes {
			gids = append(gids, gn.GroupId)
		}
		var wg sync.WaitGroup
		for _, groupNode := range input.GroupNodes {
			wg.Add(1)
			go func(clusterInfo *tbl_cluster.Cluster, groupNode *GroupNode, input *MultiUpgradeNodeInput, replicas map[string]bool) {
				masterCloud, err := multiUpgradeServer(ctx, clusterInfo, groupNode, input, replicas, false)
				if err != nil {
					log.Warnf("upgrade failed.err:%+v", err)
				}
				if len(masterCloud) > 0 {
					nodeMap.Lock()
					defer nodeMap.Unlock()
					nodes, err := mdl_node.GetNodeList(clusterInfo, groupNode.GroupId, 1)
					if err != nil {
						log.Errorf("get node list fail err:%v", err)
						return
					}
					var masterNode uint
					for m, _ := range masterCloud {
						masterNode = m
					}
					var slaveNode uint
					var slaveNodes []uint
					for _, n := range nodes {
						if n.Role != def.NODE_ROLE_SLAVE {
							continue
						}
						slaveNodes = append(slaveNodes, n.NodeId)
						if n.IDC == masterCloud[masterNode] {
							slaveNode = n.NodeId
						}
					}
					if slaveNode <= 0 && len(slaveNodes) > 0 {
						slaveNode = slaveNodes[0]
					}
					masterSlaves := make(map[string]uint, 2)
					masterSlaves["master"] = masterNode
					masterSlaves["slave"] = slaveNode
					nodeMap.Nodes[groupNode.GroupId] = masterSlaves
				}
				wg.Done()
			}(clusterInfo, groupNode, input, replicas)
		}
		wg.Wait()
		if len(nodeMap.Nodes) > 0 && input.Operation == def.OPERATION_BITALOS_UPGRADE {
			for gid, nodes := range nodeMap.Nodes {
				err = upgradeMaster(ctx, clusterInfo, gid, nodes["master"], nodes["slave"], input, replicas)
				if err != nil {
					log.Errorf("upgrade master failed gid:%d master:%d slave:%d err:%v", gid, nodes["master"], nodes["slave"], err)
				}
			}
		}
	} else if input.Operation == def.OPERATION_SUPERVISOR_STOP {
		gs := make([]string, 0)
		for _, gb := range input.GroupNodes {
			gs = append(gs, fmt.Sprintf("shard %d - nodes %s", gb.GroupId, gb.NodeIds))
		}
		replicaGroupIds, err := mdl_node.GetSlotsGroupIds(clusterInfo.Name, def.SERVICE_ID_PROXY)
		if err != nil {
			return nil, errors.New(err.Error())
		}
		var wg sync.WaitGroup
		for _, groupNode := range input.GroupNodes {
			wg.Add(1)
			go func(clusterInfo *tbl_cluster.Cluster, groupNode *GroupNode, input *MultiUpgradeNodeInput, replicas map[string]bool, replicaGroupIds map[uint]map[string]string) {
				err := multiStopNode(clusterInfo, groupNode, input, replicas, replicaGroupIds)
				if err != nil {
					log.Warnf("upgrade failed.err:%+v", err)
				}
				wg.Done()
			}(clusterInfo, groupNode, input, replicas, replicaGroupIds)
		}
		wg.Wait()
	}
	return nil, nil
}
