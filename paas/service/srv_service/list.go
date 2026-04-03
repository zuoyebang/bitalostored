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

package srv_service

import (
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

type ServiceListInput struct {
	ServiceType string `form:"serviceType"`
}

func (input *ServiceListInput) CheckParams(ctx *gin.Context) error {
	return nil
}
func (input *ServiceListInput) BuildOutput(ctx *gin.Context) (interface{}, error) {
	var output ServiceListOutput
	var err error
	var serviceList = []string{def.SERVICE_BITALOS, def.SERVICE_STORED_PROXY, def.SERVICE_STORED_DASHBOARD, def.SERVICE_STORED_FE, def.SERVICE_STORED_AGENT}
	for _, name := range serviceList {
		output.Rows = append(output.Rows, getServiceInfo(name))
	}

	tabList := []string{"cluster", "machine", "resource pool", "operation record"}
	for _, tab := range tabList {
		output.Rows = append(output.Rows, &tbl_service.Service{
			Name: tab,
		})
	}
	return output, err
}

type ServiceListOutput struct {
	Rows []*tbl_service.Service `json:"rows"`
}

func getServiceInfo(serviceName string) *tbl_service.Service {
	serviceInfo, err := tbl_service.GetInfoByName(serviceName)
	if err != nil {
		log.Warn("get service info failed.err:", err)
		return &tbl_service.Service{}
	}
	return serviceInfo
}

func (s *ServiceListOutput) selectServices(serviceNames ...string) {
	var indexList []int
	for _, name := range serviceNames {
		for index := 0; index < len(s.Rows); index++ {
			if name == s.Rows[index].Name {
				indexList = append(indexList, index)
				break
			}
		}
	}
	var newRow []*tbl_service.Service
	for _, index := range indexList {
		newRow = append(newRow, s.Rows[index])
	}
	s.Rows = newRow
}
