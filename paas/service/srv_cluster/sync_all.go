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
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/web/dashboard"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"

	"github.com/gin-gonic/gin"
)

type ClusterSyncAllInput struct {
}

var _ servicer.Servicer = new(ClusterSyncAllInput)

func (input *ClusterSyncAllInput) CheckParams(ctx *gin.Context) error {
	return nil
}

func (input *ClusterSyncAllInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	var succ int
	var fail int
	var failInfo = make([]string, 0, 10)
	clusters, err := tbl_cluster.GetClusterServerList()
	if err != nil {
		return nil, err
	}
	dashboard.SetDashboardCookie(ctx)
	for _, c := range clusters {
		err = dashboard.SyncAllGroup(ctx, c.Name)
		if err != nil {
			fail++
			failInfo = append(failInfo, c.Name)
		} else {
			succ++
		}
	}
	return &SyncAllOut{Succ: succ, Fail: fail, FailList: failInfo}, nil
}

type SyncAllOut struct {
	Succ     int      `json:"succ"`
	Fail     int      `json:"fail"`
	FailList []string `json:"failList"`
}
