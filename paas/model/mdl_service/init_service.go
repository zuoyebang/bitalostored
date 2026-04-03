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

package mdl_service

import (
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
)

func InitService() error {
	serviceInfos, err := tbl_service.GetList(-1, 0)
	if err != nil {
		log.Errorf("get service list failed.err:%v", err)
		return err
	}
	hasMatrix, hasProxy, hasDashboard, hasFE, hasAgent, hasBitalos := false, false, false, false, false, false
	for _, info := range serviceInfos {
		switch info.Name {
		case def.SERVICE_MATRIX:
			hasMatrix = true
		case def.SERVICE_STORED_PROXY:
			hasProxy = true
		case def.SERVICE_STORED_DASHBOARD:
			hasDashboard = true
		case def.SERVICE_STORED_FE:
			hasFE = true
		case def.SERVICE_STORED_AGENT:
			hasAgent = true
		case def.SERVICE_BITALOS:
			hasBitalos = true
		}
	}
	if !hasMatrix {
		_, err = tbl_service.Create(def.SERVICE_MATRIX, []int{16800, 16899}, []int{16900, 16999})
		if err != nil {
			return err
		}
	}
	if !hasProxy {
		_, err = tbl_service.Create(def.SERVICE_STORED_PROXY, []int{16600, 16601}, []int{16602, 16603})
		if err != nil {
			return err
		}
	}
	if !hasDashboard {
		_, err = tbl_service.Create(def.SERVICE_STORED_DASHBOARD, []int{16604, 16605}, []int{16606, 16607})
		if err != nil {
			return err
		}
	}
	if !hasFE {
		_, err = tbl_service.Create(def.SERVICE_STORED_FE, []int{16608, 16609}, []int{16608, 16609})
		if err != nil {
			return err
		}
	}
	if !hasAgent {
		_, err = tbl_service.Create(def.SERVICE_STORED_AGENT, []int{1, 2}, []int{1, 2})
		if err != nil {
			return err
		}
	}
	if !hasBitalos {
		_, err = tbl_service.Create(def.SERVICE_BITALOS, []int{22700, 22799}, []int{22800, 22899})
		if err != nil {
			return err
		}
	}
	return nil
}
