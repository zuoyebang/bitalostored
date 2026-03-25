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

type OfflineGroupInput struct {
	ClusterId uint `json:"clusterId"`
	GroupId   uint `json:"groupId"`
}

var _ servicer.Servicer = new(OfflineGroupInput)

func (input *OfflineGroupInput) CheckParams(ctx *gin.Context) error {
	if input.GroupId <= 0 {
		return errors.New("invalid groupId")
	}
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	return nil
}

func (input *OfflineGroupInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	return nil, tbl_group.Update(input.ClusterId, input.GroupId, tbl_group.Group{Status: def.GROUP_STATUS_OFFLINE})
}
