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

package mdl_port

import (
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_hostport"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"sort"
)

func NarrowDownPortRange(portRange []int, machineId uint) []int {
	if len(portRange) != 2 {
		return portRange
	}
	if portRange[0] >= portRange[1] {
		return portRange
	}
	machinePorts, err := tbl_hostport.GetMachinePorts(machineId, portRange[0], portRange[1])
	if err != nil {
		log.Errorf("get host port error.err:%+v", err)
		return portRange
	}
	if machinePorts == nil || len(machinePorts) == 0 {
		return portRange
	}
	log.Info("machine portRange:", portRange)
	sort.Sort(tbl_hostport.SortPort(machinePorts))
	if int(machinePorts[len(machinePorts)-1].Port)+1 < portRange[1] && int(machinePorts[len(machinePorts)-1].Port)+1 > portRange[0] {
		portRange[0] = int(machinePorts[len(machinePorts)-1].Port) + 1
	}
	log.Info("machine portRange:", portRange)
	return portRange
}
