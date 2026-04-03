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
	"github.com/zuoyebang/bitalostored/paas/model/mdl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type ApplyInput struct {
	Ids      []int `json:"ids"`
	IsManual uint  `json:"isManual"`
}

func (input *ApplyInput) CheckParams(ctx *gin.Context) error {
	if len(input.Ids) <= 0 {
		return errors.New("empty ids")
	}
	return nil
}

func (input *ApplyInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	resourceList, err := tbl_resource_pool.GetResourceByIds(input.Ids)
	if err != nil {
		log.Errorf("get resource list failed.err:%+v", err)
		return nil, err
	}
	if len(resourceList) == 0 {
		log.Warnf("empty apply list")
		return nil, nil
	}
	var manualUpdate []*tbl_resource_pool.Resource
	var noManualUpdate []*tbl_resource_pool.Resource
	var hasManual []uint
	var noManual []uint
	var manualTmp []uint
	for _, item := range resourceList {
		if item.ManualValue > 0 {
			if input.IsManual > 0 {
				hasManual = append(hasManual, item.ID)
			} else {
				manualTmp = append(manualTmp, item.ID)
			}
			if item.CgroupLimit == item.ManualValue {
				continue
			}
			manualUpdate = append(manualUpdate, item)
			continue
		}
		noManualUpdate = append(noManualUpdate, item)
		noManual = append(noManual, item.ID)
	}
	if len(hasManual) > 0 {
		err = tbl_resource_pool.ApplyManual(hasManual)
	}
	if len(noManual) > 0 {
		err = tbl_resource_pool.Applys(noManual)
	}
	if len(manualTmp) > 0 {
		err = tbl_resource_pool.ApplyManualTmp(manualTmp)
	}
	if len(manualUpdate) > 0 {
		err = mdl_resource_pool.GenerateCgroupTask(manualUpdate, false, true, false, false)
	}
	if len(noManualUpdate) > 0 {
		err = mdl_resource_pool.GenerateCgroupTask(noManualUpdate, false, false, false, false)
	}
	return nil, err
}
