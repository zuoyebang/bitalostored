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
	"github.com/zuoyebang/bitalostored/paas/service/srv_node"

	"github.com/gin-gonic/gin"
)

func CreateNode(ctx *gin.Context) {
	in := srv_node.CreateNodeInput{}
	process(ctx, &in)
}

func CreateClusterWitness(ctx *gin.Context) {
	in := srv_node.CreateClusterWitnessInput{}
	process(ctx, &in)
}

func RemoveClusterWitness(ctx *gin.Context) {
	in := srv_node.RemoveClusterWitnessInput{}
	process(ctx, &in)
}

func OfflineNode(ctx *gin.Context) {
	in := srv_node.OfflineNodeInput{}
	process(ctx, &in)
}

func NodeUpgrade(ctx *gin.Context) {
	in := srv_node.UpgradeNodeInput{}
	process(ctx, &in)
}

func NodeMultiUpgrade(ctx *gin.Context) {
	in := srv_node.MultiUpgradeNodeInput{}
	process(ctx, &in)
}

func NormalMultiUpgrade(ctx *gin.Context) {
	in := srv_node.MultiUpgradeNormalInput{}
	process(ctx, &in)
}

func NodeList(ctx *gin.Context) {
	in := srv_node.NodeListInput{}
	processGet(ctx, &in)
}

func NodeConfig(ctx *gin.Context) {
	in := srv_node.NodeConfigInput{}
	processGet(ctx, &in)
}

func UpdatePort(ctx *gin.Context) {
	in := srv_node.UpdatePortInput{}
	processGet(ctx, &in)
}

func NodeOperate(ctx *gin.Context) {
	in := srv_node.OperateNodeInput{}
	process(ctx, &in)
}
