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
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type MachineBudgetListInput struct {
}

var _ servicer.Servicer = new(MachineBudgetListInput)

func (input *MachineBudgetListInput) CheckParams(ctx *gin.Context) error {
	return nil
}

func (input *MachineBudgetListInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	list, err := tbl_machine.GetAllMachines()
	if err != nil {
		log.Warn("get config info failed.err:", err)
		return nil, err
	}
	ex := make(map[string]bool, 0)
	var budgets []string
	for _, l := range list {
		if len(l.Budget) <= 0 {
			continue
		}
		if _, ok := ex[l.Budget]; ok {
			continue
		}
		ex[l.Budget] = true
		budgets = append(budgets, l.Budget)
	}
	return MachineBudgetListOutput{budgets}, nil
}

type MachineBudgetListOutput struct {
	BudgetList []string `json:"budgetList"`
}
