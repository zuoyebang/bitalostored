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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_config"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strings"
)

type ReplaceConfInput struct {
	OldConf   string `json:"oldConf"`
	NewConf   string `json:"newConf"`
	ServiceId uint   `json:"serviceId"`
}

var _ servicer.Servicer = new(ReplaceConfInput)

func (input *ReplaceConfInput) CheckParams(ctx *gin.Context) error {
	if input.OldConf == "" {
		return errors.New("invalid oldConf")
	}
	if input.NewConf == "" {
		return errors.New("invalid newConf")
	}
	if len(input.OldConf) <= 20 {
		return errors.New("please match more chars for safe replacement")
	}
	return nil
}
func (input *ReplaceConfInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	limit, offset := 100, 0
	for {
		list, err := tbl_config.GetList(limit, offset)
		if err != nil {
			log.Warn("get config list failed.err:", err)
			return nil, err
		}
		if len(list) == 0 {
			break
		}
		var updateConfs []*tbl_config.TblConfig
		for _, conf := range list {
			if strings.Contains(conf.Content, input.OldConf) {
				conf.Content = strings.Replace(conf.Content, input.OldConf, input.NewConf, 1)
				updateConfs = append(updateConfs, conf)
			}
		}
		_, err = tbl_config.UpdateConfigs(updateConfs)
		if err != nil {
			log.Warn("update config failed.err:", err)
			return nil, err
		}
		offset = offset + limit
	}

	return "updated cluster list", nil
}
