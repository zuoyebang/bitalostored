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

package srv_cluster

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"strings"
)

type CreateFEInput struct {
	createSingleInput
}

var _ servicer.Servicer = new(CreateFEInput)

func (input *CreateFEInput) CheckParams(ctx *gin.Context) error {
	if input.RegionId <= 0 {
		return errors.New("invalid regionId")
	}
	if input.MachineId <= 0 {
		return errors.New("invalid machineId")
	}
	if input.ServiceId <= 0 {
		return errors.New("invalid serviceId")
	}
	if input.CosFileId <= 0 {
		return errors.New("invalid packageId")
	}
	if input.AssignedPort <= 0 {
		return errors.New("invalid port")
	}
	input.ClusterName = strings.TrimSpace(input.ClusterName)
	if input.ClusterName <= "" {
		return errors.New("invalid clusterName")
	}
	if input.ConfigPackId <= 1 {
		return errors.New("invalid configPackId. Do not use the default config")
	}
	input.Operation = "start"
	return nil
}

func (input *CreateFEInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	input.StoredId = 0
	return input.createSingle()
}
