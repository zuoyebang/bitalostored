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
	"github.com/zuoyebang/bitalostored/paas/service/srv_machine"

	"github.com/gin-gonic/gin"
)

func MachineAll(ctx *gin.Context) {
	in := srv_machine.MachineAllInput{}
	processGet(ctx, &in)
}

func MachineRegister(ctx *gin.Context) {
	in := srv_machine.MachineRegisterInput{}
	process(ctx, &in)
}

func MachineAddMulti(ctx *gin.Context) {
	in := srv_machine.AddMultiInput{}
	process(ctx, &in)
}

func MachineGetByIp(ctx *gin.Context) {
	in := srv_machine.MachineGetByIpInput{}
	process(ctx, &in)
}

func MachineRecovery(ctx *gin.Context) {
	in := srv_machine.MachineRecoveryInput{}
	processGet(ctx, &in)
}

func MachineUpdate(ctx *gin.Context) {
	in := srv_machine.MachineUpdateInput{}
	process(ctx, &in)
}

func MachineOffline(ctx *gin.Context) {
	in := srv_machine.MachineOfflineInput{}
	process(ctx, &in)
}

func MachineMarkOffline(ctx *gin.Context) {
	in := srv_machine.MachineMarkOfflineInput{}
	process(ctx, &in)
}

func MachineMultiRemove(ctx *gin.Context) {
	in := srv_machine.MachineMultiRemoveInput{}
	process(ctx, &in)
}

func MachineRemove(ctx *gin.Context) {
	in := srv_machine.MachineRemoveInput{}
	process(ctx, &in)
}

// func MachineRebalanced(ctx *gin.Context) {
// 	in := srv_machine.MachineRebalancedInput{}
// 	process(ctx, &in)
// }

func MachineInfos(ctx *gin.Context) {
	in := srv_machine.MachineInfosInput{}
	processGet(ctx, &in)
}

func MachineInfoList(ctx *gin.Context) {
	in := srv_machine.MachineInfoListInput{}
	processGet(ctx, &in)
}

func MachineBudgetList(ctx *gin.Context) {
	in := srv_machine.MachineBudgetListInput{}
	processGet(ctx, &in)
}

func HostPortInfos(ctx *gin.Context) {
	in := srv_machine.MachinePortStatInput{}
	processGet(ctx, &in)
}

func CheckPort(ctx *gin.Context) {
	in := srv_machine.CheckPortInput{}
	processGet(ctx, &in)
}

func MachineClusterInfo(ctx *gin.Context) {
	in := srv_machine.ClusterInfoInput{}
	processGet(ctx, &in)
}

func MachineNodeDeployInfo(ctx *gin.Context) {
	in := srv_machine.MachineDeployInfoInput{}
	processGet(ctx, &in)
}

func MachineReplicate(ctx *gin.Context) {
	in := srv_machine.MachineReplicateInput{}
	process(ctx, &in)
}

func MachineMigrate(ctx *gin.Context) {
	in := srv_machine.MachineMigrateInput{}
	process(ctx, &in)
}

func MachineRemoveProxy(ctx *gin.Context) {
	in := srv_machine.MachineRemoveProxyInput{}
	process(ctx, &in)
}
