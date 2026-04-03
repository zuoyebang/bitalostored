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

package srv_controlfe

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
)

type ConstantListInput struct {
}

var _ servicer.Servicer = new(ConstantListInput)

func (input *ConstantListInput) CheckParams(ctx *gin.Context) error {
	return nil
}

func (input *ConstantListInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	var constantList ConstantListOutput
	var dayTime int64 = 60 * 60 * 24
	constantList.TaskTimeInterval = []int64{dayTime, dayTime * 3, dayTime * 7, dayTime * 30, dayTime * 90, dayTime * 180, dayTime * 360}
	constantList.FileModeOptions = []string{"0644", "0755"}
	constantList.FileTypeOptions = []string{"main", "json", "template", "supervisord"}
	constantList.MachineStatus = []string{def.MACHINE_STATUS_ONLINE, def.MACHINE_STATUS_OFFLINE}
	constantList.IDCOptions = []string{def.IdcBaidu, def.IdcTencent, def.IdcAli, def.IdcTxcloud, def.IdcTxgz, def.IdcTxsh}
	constantList.ClusterStatus = []string{def.CLUSTER_STATUS_ONLINE, def.CLUSTER_STATUS_OFFLINE}
	constantList.GroupStatus = []string{def.GROUP_STATUS_ONLINE, def.GROUP_STATUS_OFFLINE}
	constantList.NodeStatus = []string{def.NODE_STATUS_ONLINE, def.NODE_STATUS_NEW, def.NODE_STATUS_OFFLINE}
	constantList.StrategyList = []string{def.IDCPRIORITY, def.IDCBALANCE, def.MACHINEBALANCE}
	constantList.PriorityIDCOptions = []string{def.IdcBaidu, def.IdcTencent, def.IdcAli, def.IdcTxcloud, def.IdcTxgz, def.IdcTxsh}
	constantList.ConsensusTypes = []string{def.CONSENSUS_RAFT, def.CONSENSUS_SENTINEL}
	constantList.NodeRoles = []string{def.NODE_ROLE_OBSERVER, def.NODE_ROLE_WITNESS}
	constantList.SupportServices = []string{def.SERVICE_MATRIX, def.SERVICE_STORED_PROXY, def.SERVICE_STORED_DASHBOARD,
		def.SERVICE_STORED_FE, def.SERVICE_STORED_AGENT, def.SERVICE_STORED_PAAS, def.SERVICE_STORED_MONITOR, def.SERVICE_STORED_TEST}
	constantList.MatrixOperations = def.MatrixOperationList
	constantList.ProxyOperations = def.ProxyOperationList
	constantList.DashboardOperation = def.DashboardOperationList
	constantList.FEOperations = def.FEOperationList
	constantList.AlertGroupNames = nil
	return constantList, nil
}

type ConstantListOutput struct {
	TaskTimeInterval   []int64  `json:"taskTimeInterval"`
	FileTypeOptions    []string `json:"fileTypeOptions"`
	FileModeOptions    []string `json:"fileModeOptions"`
	MachineStatus      []string `json:"machineStatus"`
	IDCOptions         []string `json:"idcOptions"`
	ClusterStatus      []string `json:"clusterStatus"`
	StrategyList       []string `json:"strategyList"`
	PriorityIDCOptions []string `json:"priorityIDCOptions"`
	GroupStatus        []string `json:"groupStatus"`
	NodeStatus         []string `json:"nodeStatus"`
	ConsensusTypes     []string `json:"consensusTypes"`
	NodeRoles          []string `json:"nodeRoles"`
	SupportServices    []string `json:"supportServices"`
	MatrixOperations   []string `json:"matrixOperations"`
	ProxyOperations    []string `json:"proxyOperations"`
	DashboardOperation []string `json:"dashboardOperations"`
	FEOperations       []string `json:"dashboardFEOperations"`
	AlertGroupNames    []string `json:"alertGroupNames"`
}
