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
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type MachineDeployInfoInput struct {
	MachineId uint `form:"machineId"`
}

var _ servicer.Servicer = new(MachineDeployInfoInput)

func (input *MachineDeployInfoInput) CheckParams(ctx *gin.Context) error {
	if input.MachineId == 0 {
		return errors.New("invalid machineId")
	}
	return nil
}

type DeployInfo struct {
	LocalIp   string         `json:"localIp"`
	IDC       string         `json:"idc"`
	ProxyLog  []*ClusterInfo `json:"proxyLogPath"`
	ServerLog []*ClusterInfo `json:"serverLogPath"`
}

type ClusterInfo struct {
	Path        string `json:"path"`
	Port        uint   `json:"port"`
	ClusterName string `json:"clusterName"`
}

func (input *MachineDeployInfoInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	mInfo, err := tbl_machine.GetInfo(input.MachineId)
	if err != nil {
		log.Warn("get machine info failed.err:", err)
		return nil, err
	}
	if mInfo.ID == 0 {
		log.Warn("invalid machine id:", input.MachineId)
		return nil, err
	}
	if mInfo.Status == def.MACHINE_STATUS_OFFLINE {
		return nil, errors.New("can only recovery a online machine.")
	}
	nodeList, err := tbl_node.GetOnlineNodes(input.MachineId)
	if err != nil {
		log.Warn("get machine nodelist failed.err:", err)
		return nil, err
	}
	output := &DeployInfo{
		LocalIp: mInfo.IP,
		IDC:     mInfo.IDC,
	}
	var proxyLogPath string

	for _, nodeInfo := range nodeList {
		clusterInfo, err := tbl_cluster.GetInfo(nodeInfo.ClusterId)
		if err != nil {
			log.Warnf("get node deploy info clusterId:%d serviceId:%d groupId:%d nodeId:%d get cluster info failed.err:%v", nodeInfo.ClusterId,
				nodeInfo.ServiceId, nodeInfo.GroupId, nodeInfo.NodeId, err)
			continue
		}
		clusterName := clusterInfo.Name
		if nodeInfo.ServiceId == def.SERVICE_ID_DASHBOARD || nodeInfo.ServiceId == def.SERVICE_ID_FE {
			continue
		}
		if nodeInfo.IsWitness {
			continue
		}
		if nodeInfo.ServiceId == def.SERVICE_ID_PROXY {
			if checkPortCluster(clusterInfo.Name, nodeInfo.ServicePort) {
				proxyLogPath = fmt.Sprintf("/home/homework/clog/bitalosproxy/%s-%d", clusterInfo.Name, nodeInfo.ServicePort)
			} else {
				proxyLogPath = fmt.Sprintf("/home/homework/clog/bitalosproxy/%s", clusterInfo.Name)
			}
			proxyCluster := &ClusterInfo{
				Path:        proxyLogPath,
				Port:        nodeInfo.ServicePort,
				ClusterName: clusterInfo.Name,
			}
			output.ProxyLog = append(output.ProxyLog, proxyCluster)
			continue
		}
		regionInfo, err := tbl_region.GetInfo(nodeInfo.RegionId)
		if err != nil {
			log.Warnf("recovery failed clusterId:%d serviceId:%d groupId:%d nodeId:%d get region info failed.err:%v", nodeInfo.ClusterId,
				nodeInfo.ServiceId, nodeInfo.GroupId, nodeInfo.NodeId, err)
			continue
		}
		regionName := regionInfo.Name
		/*
			initTasks, err := tbl_task.GetInitTask(nodeInfo.ClusterId, nodeInfo.GroupId, nodeInfo.MachineId, nodeInfo.NodeId)
			if err != nil {
				return nil, err
			}
			if len(initTasks) <= 0 {
				log.Warnf("init task missing clusterId:%d groupId:%d machineId:%d nodeId:%d", nodeInfo.ClusterId, nodeInfo.GroupId, nodeInfo.MachineId, nodeInfo.NodeId)
			} else {
				regionName = initTasks[0].TaskExt.RegionName
			}

		*/
		serverPath := fmt.Sprintf("%s/%s/stored-bitalos/%s/group-%d/node-%d-port-%d", config.GetConf().Deploy.DeployPath, regionName,
			clusterInfo.Name, nodeInfo.GroupId, nodeInfo.NodeId, nodeInfo.ServicePort)
		if clusterInfo.Name == "our-search-page" {
			clusterName = "ocr-search-page"
		}
		if clusterInfo.Name == "our-search-inv" {
			clusterName = "ocr-search-inv"
		}
		serverCluster := &ClusterInfo{
			Path:        serverPath,
			Port:        nodeInfo.ServicePort,
			ClusterName: clusterName,
		}
		output.ServerLog = append(output.ServerLog, serverCluster)
	}
	return output, nil
}

func checkPortCluster(clusterName string, servicePort uint) bool {
	if clusterName == "ocr-search" || clusterName == "ocr-search-page" || clusterName == "ocr-search-mix" || (clusterName == "ocr-search-inv" && servicePort == 8024) {
		return true
	}
	return false
}
