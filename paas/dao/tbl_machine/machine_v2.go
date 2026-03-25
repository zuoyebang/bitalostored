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

package tbl_machine

import (
	"github.com/zuoyebang/bitalostored/paas/dao"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"time"
)

func Register(ip, idc, budget, hostName string, cpuTotal, cpuSetMax, isVirtual int) (uint, error) {
	db, err := dao.GetDB(TableName)
	if err != nil {
		return 0, err
	}

	// var machines []*Machine
	// db.Where("ip = ?", ip).Find(&machines)
	// for _, m := range machines {
	// 	return m.ID, nil
	// }

	machine := &Machine{
		IP:         ip,
		IDC:        idc,
		Weight:     10,
		Budget:     budget,
		HostName:   hostName,
		Status:     def.MACHINE_STATUS_ONLINE,
		CpuTotal:   cpuTotal,
		CpuSetMax:  cpuSetMax,
		IsVirtual:  isVirtual,
		CreateTime: time.Now().Unix(),
		UpdateTime: time.Now().Unix(),
	}
	db = db.Create(machine)
	return machine.ID, db.Error
}
