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
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type BindConfigInput struct {
	ClusterId    uint `json:"clusterId"`
	ConfigPackId uint `json:"configPackId"`
}

var _ servicer.Servicer = new(BindConfigInput)

func (input *BindConfigInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	if input.ConfigPackId <= 1 {
		return errors.New("invalid configPackId.(do not bind the default config)")
	}
	return nil
}

func (input *BindConfigInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	info, err := tbl_cluster.GetInfo(input.ClusterId)
	if err != nil {
		log.Warn("get cluster info failed.err:", err)
		return nil, err
	}
	if info.ConfigPackId > 1 {
		return nil, errors.New("the cluster had bind a config.")
	}
	cs, err := tbl_config.GetListByPack(input.ConfigPackId, info.ServiceId)
	if err != nil {
		log.Warn("get config info failed.err:", err)
		return nil, err
	}
	for _, c := range cs {
		if c.ClusterId > 1 {
			return nil, errors.New("the config had bind a cluster.")
		}
	}
	err = tbl_cluster.Update(info.Id, tbl_cluster.Cluster{ConfigPackId: input.ConfigPackId})
	if err != nil {
		log.Warn("update cluster configPackId failed.err:", err)
		return nil, err
	}

	err = tbl_config.UpdateClusterId(input.ConfigPackId, info.Id, info.ServiceId)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
