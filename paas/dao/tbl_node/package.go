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

package tbl_node

import (
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
)

func IsPackageInUse(serviceId, packageId uint) bool {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return true
	}
	var res []*Node
	db = db.Where("service_id = ? and package_id = ? and status = ?", serviceId, packageId, def.NODE_STATUS_ONLINE).Find(&res)
	return len(res) != 0
}

func IsFileInUse(serviceId, cosFileId uint) bool {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return true
	}
	var res []*Node
	db = db.Where("service_id = ? and cos_file_id = ? and status = ?", serviceId, cosFileId, def.NODE_STATUS_ONLINE).Find(&res)
	return len(res) != 0
}
