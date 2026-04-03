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

package srv_cluster

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"sort"
)

type ProxyInfoInput struct {
	ClusterId uint `form:"clusterId"`
}

var _ servicer.Servicer = new(ProxyInfoInput)

func (input *ProxyInfoInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	return nil
}

func (input *ProxyInfoInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	var output ProxyInfoOutput
	var err error
	clusterInfo, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil {
		log.Error("get cluster info failed.err:", err)
		return nil, err
	}
	regionInfo, err := tbl_region.GetInfo(clusterInfo.RegionId)
	if err != nil {
		log.Error("get region info failed.err:", err)
		return nil, err
	}
	regionName := regionInfo.Name
	var newRegionInfo *tbl_region.Region
	if regionInfo.NewId > 0 {
		newRegionInfo, err = tbl_region.GetInfo(regionInfo.NewId)
		if err != nil {
			log.Error("get new region info failed.err:", err)
			return nil, err
		}
		regionName = regionName + "(" + newRegionInfo.Name + ")"
	}
	// serviceInfo, err := tbl_service.GetInfo(clusterInfo.ServiceId)
	// if err != nil {
	// 	log.Error("get service info failed.err:", err)
	// }
	nodeInfos, err := tbl_node.GetListByCluster(input.ClusterId)
	if err != nil {
		log.Warnf("failed to get cluster nodes.err:%+v", err)
		return nil, err
	}
	for _, nodeInfo := range nodeInfos {
		machineInfo, err := tbl_machine.GetInfo(nodeInfo.MachineId)
		if err != nil {
			log.Warnf("failed to get machine info.err:%+v", err)
		}
		var is = "no"
		if !clusterInfo.IsStored1 {
			is = "yes"
		}
		if nodeInfo.RegionId == regionInfo.NewId {
			regionName = newRegionInfo.Name
		}
		output.ProxyInfos = append(output.ProxyInfos, NodeInfo{
			Node:          nodeInfo,
			IP:            machineInfo.IP,
			ClusterName:   clusterInfo.Name,
			RegionName:    regionName,
			IsMatrixProxy: is,
			Idc:           machineInfo.IDC,
		})
	}
	sort.Slice(output.ProxyInfos, func(i, j int) bool {
		return output.ProxyInfos[i].IP < output.ProxyInfos[j].IP
	})
	return output, nil
}

type ProxyInfoOutput struct {
	ProxyInfos []NodeInfo `json:"rows"`
}

type NodeInfo struct {
	*tbl_node.Node
	IP            string `json:"ip"`
	ClusterName   string `json:"clusterName"`
	RegionName    string `json:"regionName"`
	IsMatrixProxy string `json:"matrixProxy"`
	Idc           string `json:"idc"`
}
