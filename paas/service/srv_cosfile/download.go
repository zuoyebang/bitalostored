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
	"net/http"
	"os"
	"path/filepath"

	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"

	"github.com/gin-gonic/gin"
)

type FileDownloadInput struct {
	FilePath string `form:"filePath"`
}

var _ servicer.Servicer = new(FileDownloadInput)

func (input *FileDownloadInput) CheckParams(ctx *gin.Context) error {
	if input.FilePath == "" {
		return errors.New("invalid filePath")
	}
	return nil
}

func (input *FileDownloadInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	fileDir := config.GetConf().BuildServer.LocalDir

	fullPath := filepath.Join(fileDir, input.FilePath)

	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		return nil, errors.New(fmt.Sprintf("file not found: %s", fullPath))
	}

	content, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("failed to read file: %v", err))
	}

	ctx.Header("Content-Description", "File Transfer")
	ctx.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filepath.Base(input.FilePath)))
	ctx.Header("Content-Type", "application/octet-stream")
	ctx.Header("Content-Transfer-Encoding", "binary")
	ctx.Header("Content-Length", fmt.Sprintf("%d", len(content)))
	ctx.Header("Cache-Control", "no-cache")

	ctx.Data(http.StatusOK, "application/octet-stream", content)

	return nil, nil
}
