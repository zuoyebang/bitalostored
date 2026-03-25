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

package controller

import (
	"github.com/zuoyebang/bitalostored/paas/service/srv_cluster"

	"github.com/gin-gonic/gin"
)

func CreateMatrixCluster(ctx *gin.Context) {
	in := srv_cluster.CreateMatrixInput{}
	process(ctx, &in)
}

func AlignProxyCluster(ctx *gin.Context) {
	in := srv_cluster.AlignProxyInput{}
	process(ctx, &in)
}

func CreateProxyCluster(ctx *gin.Context) {
	in := srv_cluster.CreateProxyInput{}
	process(ctx, &in)
}

func CreateDashboardCluster(ctx *gin.Context) {
	in := srv_cluster.CreateDashboardInput{}
	process(ctx, &in)
}

func CreateFECluster(ctx *gin.Context) {
	in := srv_cluster.CreateFEInput{}
	process(ctx, &in)
}

func MatrixInfo(ctx *gin.Context) {
	in := srv_cluster.MatrixInfoInput{}
	processGet(ctx, &in)
}

func ProxyInfo(ctx *gin.Context) {
	in := srv_cluster.ProxyInfoInput{}
	processGet(ctx, &in)
}

func ClusterList(ctx *gin.Context) {
	in := srv_cluster.ClusterListInput{}
	processGet(ctx, &in)
}

func ClusterIPList(ctx *gin.Context) {
	in := srv_cluster.ClusterIPListInput{}
	processGet(ctx, &in)
}

func ClusterSyncAll(ctx *gin.Context) {
	in := srv_cluster.ClusterSyncAllInput{}
	processGet(ctx, &in)
}

func ClusterName(ctx *gin.Context) {
	in := srv_cluster.ClusterNameInput{}
	processGet(ctx, &in)
}

func StoredList(ctx *gin.Context) {
	in := srv_cluster.StoredListInput{}
	processGet(ctx, &in)
}

func ReplaceDashboard(ctx *gin.Context) {
	in := srv_cluster.ReplaceDashboardInput{}
	process(ctx, &in)
}

func Expand(ctx *gin.Context) {
	in := srv_cluster.ExpandInput{}
	process(ctx, &in)
}

func UpdateMonitor(ctx *gin.Context) {
	in := srv_cluster.UpdateMonitorInput{}
	process(ctx, &in)
}

func RemoveCluster(ctx *gin.Context) {
	in := srv_cluster.RemoveClusterInput{}
	process(ctx, &in)
}

func BindDepartment(ctx *gin.Context) {
	in := srv_cluster.BindDepartmentInput{}
	process(ctx, &in)
}

func MarkOffline(ctx *gin.Context) {
	in := srv_cluster.MarkOfflineInput{}
	processGet(ctx, &in)
}

func DeleteOffline(ctx *gin.Context) {
	in := srv_cluster.DeleteOfflineInput{}
	processGet(ctx, &in)
}

func DepartmentList(ctx *gin.Context) {
	in := srv_cluster.DepartmentListInput{}
	processGet(ctx, &in)
}

func Offline(ctx *gin.Context) {
	in := srv_cluster.OfflineInput{}
	process(ctx, &in)
}

func DeployOverview(ctx *gin.Context) {
	in := srv_cluster.DeployOverviewInput{}
	process(ctx, &in)
}

func ClusterDeployInfo(ctx *gin.Context) {
	in := srv_cluster.ClusterDeployInfoInput{}
	process(ctx, &in)
}

func ClusterDeployServerDetail(ctx *gin.Context) {
	in := srv_cluster.ClusterDeployServerDetailInput{}
	process(ctx, &in)
}

func ClusterCreateAll(ctx *gin.Context) {
	in := srv_cluster.CreateAllInput{}
	process(ctx, &in)
}
