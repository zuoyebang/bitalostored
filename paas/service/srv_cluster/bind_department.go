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
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
)

type BindDepartmentInput struct {
	ClusterId  uint   `json:"clusterId"`
	Department string `json:"department"`
}

var _ servicer.Servicer = new(BindDepartmentInput)

func (input *BindDepartmentInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	if input.Department == "" {
		return errors.New("invalid department")
	}
	return nil
}

func (input *BindDepartmentInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	return nil, tbl_cluster.Update(input.ClusterId, tbl_cluster.Cluster{Department: input.Department})
}
