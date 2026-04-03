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

package tbl_opsactionlog

import (
	"github.com/zuoyebang/bitalostored/paas/dao"
)

const TableName = "tblOpsActionLog"

type OpsActionLog struct {
	ID          uint   `gorm:"column:id" json:"id"`
	Ip          string `gorm:"column:ip" json:"ip"`
	Port        uint   `gorm:"column:port" json:"port"`
	ClusterName string `gorm:"column:cluster_name" json:"clusterName"`
	ActionType  int    `gorm:"column:action_type" json:"actionType"`
	OpName      string `gorm:"column:op_name" json:"opName"`
	UpdateTime  int64  `gorm:"column:update_time" json:"-"`
	CreateTime  int64  `gorm:"column:create_time" json:"createTime"`
}

func Create(actionLog *OpsActionLog) error {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return err
	}
	db = db.Create(actionLog)
	return db.Error
}
