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

package srv_resource_pool

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type ControlCostInput struct {
	Control []*Cost `json:"manual"`
}

type Cost struct {
	Id  uint  `json:"id"`
	Cpu int64 `json:"cpu"`
}

func (input *ControlCostInput) CheckParams(ctx *gin.Context) error {
	if len(input.Control) <= 0 {
		return errors.New("empty params")
	}
	return nil
}

func (input *ControlCostInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	for _, item := range input.Control {
		err := tbl_resource_pool.UpdateCostById(item.Cpu, item.Id)
		if err != nil {
			log.Errorf("id: %v update failed", item.Id)
		}
	}
	return nil, nil
}
