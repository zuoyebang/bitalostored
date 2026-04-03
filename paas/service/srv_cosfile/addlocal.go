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

package srv_cosfile

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cosfile"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
)

type AddLocalFileInput struct {
	FileName  string `json:"fileName"`
	Version   string `json:"version"`
	ServiceId uint   `json:"serviceId"`
}

var _ servicer.Servicer = new(AddLocalFileInput)

func (input *AddLocalFileInput) CheckParams(ctx *gin.Context) error {
	if input.FileName == "" {
		return errors.New("invalid fileName")
	}
	if input.Version == "" {
		return errors.New("invalid version")
	}
	if input.ServiceId == 0 {
		return errors.New("invalid serviceId")
	}
	return nil
}

func (input *AddLocalFileInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	name := fmt.Sprintf("bin/%s", input.FileName)
	_, err := tbl_cosfile.Create(name, input.FileName, "lan", "0755", "", input.Version, input.ServiceId)
	if err != nil {
		return nil, err
	}
	if input.ServiceId == def.SERVICE_ID_FE {
		feKey := fmt.Sprintf("%s.tar.zz", input.FileName)
		_, err := tbl_cosfile.Create(feKey, feKey, "lan-compress", "0644", "", input.Version, input.ServiceId)
		if err != nil {
			return nil, err
		}
	}
	return "success", nil
}
