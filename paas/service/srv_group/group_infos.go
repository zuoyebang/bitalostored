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

package srv_group

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_group"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
)

type GroupInfosInput struct {
	ClusterId   uint   `form:"clusterId"`
	GroupIdList []uint `form:"groupIdList"`
}

var _ servicer.Servicer = new(GroupInfosInput)

func (input *GroupInfosInput) CheckParams(ctx *gin.Context) error {
	if len(input.GroupIdList) == 0 && input.ClusterId <= 0 {
		return errors.New(def.ErrMsg[def.PARAM_ERROR])
	}
	return nil
}

func (input *GroupInfosInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	if len(input.GroupIdList) == 0 {
		groups, err := tbl_group.GetList(input.ClusterId, "", 10, 0)
		return GroupInfo{groups}, err
	}
	// length > 0
	var groups []*tbl_group.Group
	for _, nodeId := range input.GroupIdList {
		nodeInfo, err := tbl_group.GetInfo(input.ClusterId, nodeId)
		if err != nil {
			return nil, err
		}
		groups = append(groups, nodeInfo)
	}
	return GroupInfo{groups}, nil
}

type GroupInfo struct {
	GroupInfos []*tbl_group.Group `json:"rows"`
}
