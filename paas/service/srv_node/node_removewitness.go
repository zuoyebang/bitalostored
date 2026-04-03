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

type RemoveClusterWitnessInput struct {
	ClusterId uint   `json:"clusterId"`
	IDC       string `json:"idc"`
}

var _ servicer.Servicer = new(RemoveClusterWitnessInput)

func (input *RemoveClusterWitnessInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	if input.IDC == "" {
		return errors.New("invalid idc")
	}
	return nil
}

func (input *RemoveClusterWitnessInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	cnt, err := mdl_node.RemoveOneWitness(ctx, input.ClusterId, input.IDC)
	if err != nil {
		return nil, err
	}
	var r RemoveClusterWitnessOutput
	r.Count = cnt.(int)
	return r, nil
}

type RemoveClusterWitnessOutput struct {
	Count int `json:"count"`
}
