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
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
)

type CreateClusterWitnessInput struct {
	ClusterId uint   `json:"clusterId"`
	CosFileId uint   `json:"packageId"`
	IDC       string `json:"idc"`
}

var _ servicer.Servicer = new(CreateClusterWitnessInput)

func (input *CreateClusterWitnessInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	if input.CosFileId <= 0 {
		return errors.New("invalid packageId")
	}
	if input.IDC == "" {
		return errors.New("invalid idc")
	}
	return nil
}

func (input *CreateClusterWitnessInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	return mdl_node.CreateAllWitness(input.ClusterId, input.CosFileId, input.IDC)
}
