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
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cosfile"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/utils/toolkit"
)

type UpgradeNodeInput struct {
	ClusterId    uint   `json:"clusterId"`
	GroupId      uint   `json:"groupId"`
	NodeId       uint   `json:"nodeId"`
	CosFileId    uint   `json:"packageId"`
	UpdateConfig string `json:"updateConfig"`
	Operation    string `json:"operation"`
	Version      string `json:"version"`
}

var _ servicer.Servicer = new(UpgradeNodeInput)

func (input *UpgradeNodeInput) CheckParams(ctx *gin.Context) error {
	if input.ClusterId <= 0 {
		return errors.New("invalid clusterId")
	}
	if input.GroupId <= 0 {
		return errors.New("invalid groupId")
	}
	if input.NodeId <= 0 {
		return errors.New("invalid nodeId")
	}
	if input.CosFileId <= 0 {
		return errors.New("invalid packageId")
	}
	if input.Operation == "" {
		return errors.New("invalid operation")
	}
	return nil
}

func (input *UpgradeNodeInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	cosFile, err := tbl_cosfile.GetCosFile(input.CosFileId)
	if err != nil {
		return nil, errors.New("multigrade get cos file failed")
	}
	if def.IsServer(cosFile.ServiceId) {
		canUpgrade := toolkit.CheckVersion(input.Version, cosFile.Version)
		if !canUpgrade {
			return nil, errors.New(fmt.Sprintf("can't upgrade %s to %s", input.Version, cosFile.Version))
		}
		input.Version = cosFile.Version
		return upgradeServerNode(ctx, input)
	}

	_, err = upgradeNormalNode(input.ClusterId, input.NodeId, input.CosFileId, input.Operation, input.UpdateConfig)
	return nil, err
}
