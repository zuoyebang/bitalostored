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
	"github.com/zuoyebang/bitalostored/paas/service/srv_region"

	"github.com/gin-gonic/gin"
)

func RegionList(ctx *gin.Context) {
	in := srv_region.RegionListInput{}
	processGet(ctx, &in)
}

func CreateRegion(ctx *gin.Context) {
	in := srv_region.CreateRegionInput{}
	process(ctx, &in)
}

func BindMachines(ctx *gin.Context) {
	in := srv_region.BindMachinesInput{}
	process(ctx, &in)
}

func UnbindMachine(ctx *gin.Context) {
	in := srv_region.UnbindMachineInput{}
	process(ctx, &in)
}

func RemoveRegion(ctx *gin.Context) {
	in := srv_region.RemoveRegionInput{}
	process(ctx, &in)
}

func RemoveRegionMachines(ctx *gin.Context) {
	in := srv_region.RemoveRegionMachinesInput{}
	process(ctx, &in)
}
