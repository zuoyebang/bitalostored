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

package srv_operation

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_operation"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type ListInput struct {
	Uid       string `form:"uid"`
	Module    string `form:"module"`
	StartTime int64  `form:"startTime"`
	EndTime   int64  `form:"endTime"`
	Page      int    `form:"page"`
	Num       int    `form:"num"`
}

type OperationListOutput struct {
	Count   int64                            `json:"count"`
	Rows    []*tbl_operation.OperationRecord `json:"rows"`
	UidList []string                         `json:"uidList"`
}

var _ servicer.Servicer = new(ListInput)

func (input *ListInput) CheckParams(ctx *gin.Context) error {
	if input.Page == 0 {
		input.Page = 1
	}
	if input.Num == 0 {
		input.Num = 20
	}
	return nil
}

func (input *ListInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	operationList, err := tbl_operation.GetList(input.Uid, input.Module, input.StartTime, input.EndTime, input.Page, input.Num)
	if err != nil {
		log.Warnf("get operation uid:%s failed.err:%v", input.Uid, err)
		return "", err
	}
	var output OperationListOutput
	output.Count, _ = tbl_operation.GetCount(input.Uid)
	output.Rows = operationList
	uidList, err := tbl_operation.GetUidList()
	if err != nil {
		log.Warnf("get uid list failed, err:%v", err)
	}
	var uids []string
	for _, uid := range uidList {
		if len(uid.Uid) > 0 {
			uids = append(uids, uid.Uid)
		}
	}
	output.UidList = uids
	return output, nil
}
