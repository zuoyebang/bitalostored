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
	"github.com/zuoyebang/bitalostored/paas/service/srv_cosfile"

	"github.com/gin-gonic/gin"
)

func FileList(ctx *gin.Context) {
	in := srv_cosfile.FileListInput{}
	processGet(ctx, &in)
}

func FileBuild(ctx *gin.Context) {
	in := srv_cosfile.BuildFileInput{}
	process(ctx, &in)
}

func FileRemove(ctx *gin.Context) {
	in := srv_cosfile.FileRemoveInput{}
	process(ctx, &in)
}

func FileDownload(ctx *gin.Context) {
	in := srv_cosfile.FileDownloadInput{}
	processGet(ctx, &in)
}

func FileAddLocal(ctx *gin.Context) {
	in := srv_cosfile.AddLocalFileInput{}
	process(ctx, &in)
}
