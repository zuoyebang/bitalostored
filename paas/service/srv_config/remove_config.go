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

package srv_config

import (
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_config"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type RemoveConfigInput struct {
	ClusterId    uint `json:"clusterId"`
	ConfigPackId uint `json:"configPackId"`
}

var _ servicer.Servicer = new(RemoveConfigInput)

func (input *RemoveConfigInput) CheckParams(ctx *gin.Context) error {
	return nil
}

func (input *RemoveConfigInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	if input.ConfigPackId == 1 {
		return "could not remove default config.", nil
	}
	if input.ClusterId != 0 {
		info, err := tbl_cluster.GetInfo(input.ClusterId)
		if err != nil {
			log.Warn("get cluster info failed.err:", err)
			return nil, err
		}
		if info.Status == def.CLUSTER_STATUS_ONLINE {
			return nil, errors.New("cloud remove offline cluster config only.")
		}
	}
	return nil, tbl_config.DeleteConfigs(input.ClusterId, input.ConfigPackId)
}
