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
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_region"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_regionmachine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type ReplaceDashboardInput struct {
	ClusterId  uint   `json:"clusterId"`
	MachineId  uint   `json:"machineId"`
	CosFileId  uint   `json:"version"`
	PackageId  uint   `json:"packageId"`
	Operation  string `json:"operation"`
	StoredAuth string `json:"storedAuth"`
}

var _ servicer.Servicer = new(ReplaceDashboardInput)

func (input *ReplaceDashboardInput) CheckParams(ctx *gin.Context) error {
	if input.MachineId <= 0 {
		return errors.New("invalid machineId")
	}
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	// if input.PackageId <= 0 {
	// 	return errors.New("invalid packageId")
	// }
	if input.Operation == "" {
		return errors.New("invalid operation")
	}
	return nil
}

func (input *ReplaceDashboardInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	clusterInfo, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil {
		log.Warn("get cluster info failed")
		return nil, err
	}
	serviceInfo, err := tbl_service.GetInfo(clusterInfo.ServiceId)
	if err != nil {
		log.Warnf("get service info failed.err:%+v", err)
		return nil, err
	}
	if serviceInfo.Name != def.SERVICE_STORED_DASHBOARD {
		return nil, errors.New("should be dasbboard cluster.")
	}
	nodeInfos, err := tbl_node.GetListByCluster(input.ClusterId)
	if err != nil || len(nodeInfos) != 1 {
		log.Warnf("get node info failed.err:%+v.nodeInfos:%+v", err, nodeInfos)
		return nil, errors.New(fmt.Sprintf("%+v.%+v", err, nodeInfos))
	}
	regionId := clusterInfo.RegionId
	r, err := tbl_region.GetInfo(clusterInfo.RegionId)
	if err != nil {
		return nil, err
	}
	if r.NewId > 0 {
		regionId = r.NewId
	}
	machineList, err := tbl_regionmachine.GetMachinesByRegion(regionId)
	if err != nil {
		log.Warnf("get region machine list failed.err:%+v", err)
		return nil, err
	}
	hit := false
	for _, m := range machineList {
		if m == input.MachineId {
			hit = true
			break
		}
	}
	if !hit {
		return nil, errors.New("machineId should be in same region with former cluster")
	}
	createDashboardInput := createSingleInput{
		RegionId:     regionId,
		ServiceId:    clusterInfo.ServiceId,
		CosFileId:    input.CosFileId,
		ClusterName:  clusterInfo.Name,
		AssignedPort: nodeInfos[0].ServicePort,
		StoredId:     1,
		MachineId:    input.MachineId,
		Operation:    input.Operation,
		StoredAuth:   input.StoredAuth,
		ConfigPackId: clusterInfo.ConfigPackId,
	}
	dt, err := createDashboardInput.createSingle()
	if err != nil {
		log.Warnf("get create new dashboard failed.err:%+v", err)
		return nil, err
	}

	// newCluster, err := tbl_cluster.GetInfo(dt.(CreateClusterOutput).ClusterId)
	return nil, tbl_cluster.UpdateClusterStoredId(clusterInfo.StoredId, dt.(CreateSingleOutput).StoredId, clusterInfo.ServiceId)
}
