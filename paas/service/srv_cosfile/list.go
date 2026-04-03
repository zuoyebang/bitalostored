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
	"github.com/gin-gonic/gin"
	jsoniter "github.com/json-iterator/go"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cosfile"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_node"
	"github.com/zuoyebang/bitalostored/paas/service/servicer"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
)

type FileListInput struct {
	ServiceId uint `form:"serviceId"`
	ClusterId int  `form:"clusterId"`
}

var _ servicer.Servicer = new(FileListInput)

func (input *FileListInput) CheckParams(ctx *gin.Context) error {
	if input.ServiceId == 0 {
		return errors.New("invalid serviceId")
	}
	return nil
}

type FileListResult struct {
	*tbl_cosfile.CosFile
	ClusterList string `json:"clusterList"`
}

func (input *FileListInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	list, err := tbl_cosfile.GetList(input.ServiceId, input.ClusterId)
	if err != nil {
		return list, err
	}
	res := make([]FileListResult, 0)
	for _, v := range list {
		clusterList, err := mdl_node.GetClusterList(v.ID)
		if err != nil {
			res = append(res, FileListResult{
				CosFile:     v,
				ClusterList: "",
			})
		} else {
			if len(clusterList) == 0 {
				res = append(res, FileListResult{
					CosFile:     v,
					ClusterList: "",
				})
				continue
			}
			strClusterList, _ := jsoniter.Marshal(clusterList)
			res = append(res, FileListResult{
				CosFile:     v,
				ClusterList: string(strClusterList),
			})
		}
	}
	return res, nil
}
