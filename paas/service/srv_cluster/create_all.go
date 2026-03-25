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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_cluster"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"strings"

	"github.com/gin-gonic/gin"
)

type CreateAllInput struct {
	RegionId        uint   `json:"regionId"`
	ClusterName     string `json:"clusterName"`
	ServerCosFileId uint   `json:"serverCosFileId"`
	BudgetUnit      string `json:"budgetUnit"`
	NodePerGroup    uint   `json:"nodePerGroup"`
}

var _ servicer.Servicer = new(CreateAllInput)

func (input *CreateAllInput) CheckParams(ctx *gin.Context) error {
	if input.RegionId <= 0 {
		return errors.New("invalid regionId")
	}
	if input.ServerCosFileId <= 0 {
		return errors.New("invalid serverCosFileId")
	}
	input.ClusterName = strings.TrimSpace(input.ClusterName)
	if input.ClusterName <= "" {
		return errors.New("invalid clusterName")
	}
	if input.BudgetUnit <= "" {
		return errors.New("invalid budgetUint")
	}
	if input.NodePerGroup <= 0 {
		return errors.New("invalid nodePerGroup")
	}
	return nil
}

func (input *CreateAllInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	info, _ := tbl_cluster.GetInfoByName(input.ClusterName, def.SERVICE_ID_DASHBOARD)
	if info != nil && info.Name == input.ClusterName {
		return nil, errors.New("cluster name exists")
	}
	err := mdl_cluster.PrepareClusterEnv(input.RegionId, input.ClusterName, input.ServerCosFileId, input.BudgetUnit, input.NodePerGroup)
	return nil, err
}
