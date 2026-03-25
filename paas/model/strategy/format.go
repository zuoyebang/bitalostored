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

package strategy

import (
	"errors"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_regionmachine"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"sort"
)

func FormatMachines(regionId, serviceId, clusterId uint, ips []string, isWitness bool) (map[string][]FormatMachine, error) {
	var err error
	var machineList []*tbl_machine.Machine
	formatList := make(map[string][]FormatMachine, 0)
	if len(ips) > 0 {
		machineList, err = tbl_machine.GetMachinesByIpList(ips)
	} else {
		machineIds, e := tbl_regionmachine.GetMachinesByRegion(regionId)
		if e != nil {
			log.Warnf("failed to get region machines.err:%+v", e)
			return nil, e
		}
		machineList, err = tbl_machine.GetList(machineIds)
	}
	if err != nil {
		log.Warn("get machine info failed.err:", err)
		return nil, err
	}
	for _, m := range machineList {
		if m.Status == def.MACHINE_STATUS_OFFLINE {
			continue
		}
		var nodes []*tbl_node.Node
		if clusterId == 0 {
			nodes, err = tbl_node.GetMachineOnlineNodes(m.ID, serviceId, isWitness)
		} else {
			nodes, err = tbl_node.GetMachineOnlineClusterNodes(m.ID, clusterId, isWitness)
		}
		if err != nil {
			log.Warn("get machines online nodes failed.err", err)
			return nil, err
		}
		formatList[m.IDC] = append(formatList[m.IDC], FormatMachine{
			Machine: m,
			Nodes:   len(nodes),
		})
	}
	for k := range formatList {
		sort.Sort(SorterMachines(formatList[k]))
	}
	return formatList, nil
}

func SplitMachinesByGroup(priorityIDC string, groupNum, nodeNum int, machinesMap map[string][]FormatMachine) ([][]FormatMachine, error) {
	if groupNum == 0 || nodeNum == 0 {
		return nil, errors.New("invalid groupNum or nodeNum")
	}
	var allMachines [][]FormatMachine
	if k, ok := machinesMap[priorityIDC]; ok {
		allMachines = append(allMachines, k)
	}
	for k, v := range machinesMap {
		if k == priorityIDC {
			continue
		}
		allMachines = append(allMachines, v)
	}
	var distributedMachines [][]FormatMachine
	rowIndexes := make([]int, len(machinesMap))

	for i := 0; i < groupNum; i++ {
		columnIndexes := 0
		var fms []FormatMachine
		fms = append(fms, allMachines[columnIndexes][rowIndexes[columnIndexes]])
		rowIndexes[columnIndexes]++
		if rowIndexes[columnIndexes] >= len(allMachines[columnIndexes]) {
			rowIndexes[columnIndexes] = 0
		}
		columnIndexes++
		if columnIndexes >= len(machinesMap) {
			columnIndexes = 0
		}
		for j := 0; j < nodeNum-1; j++ {
			fms = append(fms, allMachines[columnIndexes][rowIndexes[columnIndexes]])
			rowIndexes[columnIndexes]++
			if rowIndexes[columnIndexes] >= len(allMachines[columnIndexes]) {
				rowIndexes[columnIndexes] = 0
			}
			columnIndexes++
			if columnIndexes >= len(machinesMap) {
				columnIndexes = 0
			}
		}
		distributedMachines = append(distributedMachines, fms)
	}
	return distributedMachines, nil
}

type FormatMachine struct {
	*tbl_machine.Machine
	Nodes int
}

type SorterMachines []FormatMachine

func (l SorterMachines) Len() int {
	return len(l)
}

func (l SorterMachines) Less(i, j int) bool {
	return l[i].Nodes < l[j].Nodes
}

func (l SorterMachines) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
