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

package mdl_resource_pool

import (
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_machine"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

func FormatMachineCgroup(servicePort, clusterId, mid uint, idc string) string {
	res := make(map[uint]map[string]string, 0) //servicePort->cpu->value
	res[servicePort] = make(map[string]string, 0)
	list, err := tbl_resource_pool.GetNodeResource(clusterId, idc)
	if err != nil {
		log.Errorf("GetNodeResource failed, err:%v", err)
		return ""
	}
	if len(list) <= 0 {
		log.Warn("GetNodeResource empty")
		return ""
	}
	for _, resource := range list {
		res[servicePort][resource.MetricName] = strconv.FormatInt(resource.CgroupLimit, 10)
		if resource.CpuSetType == def.EXCLUSIVE_CPU {
			cpus, _ := AssignCpuSet(mid, servicePort, int(resource.CgroupLimit), int(resource.MinCpu), int(resource.MaxCpu))
			res[servicePort][def.CGROUP_TASK_CPU_SET] = cpus
		}
		if resource.CpuSetType == def.SHARE_CPU {
			machineInfo, err := tbl_machine.GetInfo(mid)
			if err != nil {
				log.Errorf("get machine:%d err:%v", mid, err)
			} else {
				shareCpus := fmt.Sprintf("%d-%d", machineInfo.CpuSetMax+1, machineInfo.CpuTotal-1)
				res[servicePort][def.CGROUP_TASK_CPU_SHARE] = shareCpus
			}
		}
	}
	r, _ := jsoniter.MarshalToString(res)
	return r
}

func GenerateCgroupTask(resourceList []*tbl_resource_pool.Resource, needUpdateResource, useManualValue, useLimitValue, needShare bool) error {
	//[machineid][servicePort][cpu]=>value
	//[machineid][servicePort][cpu_set]=>string value
	formatV := make(map[uint]map[uint]map[string]string, 0)
	notSetCpu := make(map[uint]map[uint]int, 0)
	shareCpu := make(map[uint]map[uint]int, 0)
	resourceCpuScope := make(map[string]map[string]int64, 0) //mid_port,minCpu,0
	for _, list := range resourceList {
		nodes, err := tbl_node.GetOnlineClusterMachine(list.ClusterId, list.ServiceId, false)
		if err != nil {
			log.Errorf("apply failed clusterId:%d serviceId:%d err:%+v", list.ClusterId, list.ServiceId, err)
			continue
		}
		machineIds := make([]uint, 0)
		for _, node := range nodes {
			machineIds = append(machineIds, node.MachineId)
		}
		machineInfos, err := tbl_machine.GetList(machineIds)
		if err != nil {
			log.Errorf("get machineInfos:%v faild err:%v", machineIds, err)
		}
		delMids := make(map[uint]int, 0)
		if len(machineInfos) > 0 {
			for _, machineInfo := range machineInfos {
				if machineInfo.IDC != list.IDC {
					delMids[machineInfo.ID] = 1
				}
			}
		}
		for _, node := range nodes {
			if _, ok := delMids[node.MachineId]; ok {
				continue
			}
			mp := fmt.Sprintf("%d_%d", node.MachineId, node.ServicePort)
			if resourceCpuScope[mp] == nil {
				resourceCpuScope[mp] = make(map[string]int64, 0)
			}
			resourceCpuScope[mp]["minCpu"] = list.MinCpu
			resourceCpuScope[mp]["maxCpu"] = list.MaxCpu
			if formatV[node.MachineId] == nil {
				formatV[node.MachineId] = make(map[uint]map[string]string, 0)
			}
			if formatV[node.MachineId][node.ServicePort] == nil {
				formatV[node.MachineId][node.ServicePort] = make(map[string]string, 0)
			}
			if useManualValue {
				formatV[node.MachineId][node.ServicePort][list.MetricName] = strconv.FormatInt(list.ManualValue, 10)
			} else if useLimitValue {
				formatV[node.MachineId][node.ServicePort][list.MetricName] = strconv.FormatInt(list.CgroupLimit, 10)
			} else {
				formatV[node.MachineId][node.ServicePort][list.MetricName] = strconv.FormatInt(list.SuggestValue, 10)
			}
			if list.CpuSetType == def.NOT_SET_CPU {
				if notSetCpu[node.MachineId] == nil {
					notSetCpu[node.MachineId] = make(map[uint]int, 0)
				}
				notSetCpu[node.MachineId][node.ServicePort] = 1
			}
			if list.CpuSetType == def.SHARE_CPU {
				if shareCpu[node.MachineId] == nil {
					shareCpu[node.MachineId] = make(map[uint]int, 0)
				}
				shareCpu[node.MachineId][node.ServicePort] = 1
			}
		}

		if needUpdateResource {
			resource := &tbl_resource_pool.Resource{
				ApplyTime: time.Now().Unix(),
			}
			if useLimitValue {
				err = tbl_resource_pool.Update(list.ID, resource)
			} else {
				err = tbl_resource_pool.Apply(list.ID, resource)
			}
			if err != nil {
				log.Warnf("update applytime fail, err:%v", err)
			}
		}
	}
	mPorts := make(map[uint][]uint, 0)
	for mid, v := range formatV {
		for port, value := range v {
			if _, ok := notSetCpu[mid][port]; ok {
				continue
			}
			if _, ok := shareCpu[mid][port]; ok {
				if !needShare {
					continue
				}
			}
			if mPorts[mid] == nil {
				mPorts[mid] = make([]uint, 0)
			}
			mPorts[mid] = append(mPorts[mid], port)
			intCpu, _ := strconv.ParseInt(value[def.CGROUP_TASK_CPU], 10, 64)
			mp := fmt.Sprintf("%d_%d", mid, port)
			minCpu := 0
			maxCpu := 0
			if _, ok := resourceCpuScope[mp]; ok {
				minCpu = int(resourceCpuScope[mp]["minCpu"])
				maxCpu = int(resourceCpuScope[mp]["maxCpu"])
			}
			cpuSet, isUpdate := AssignCpuSet(mid, port, int(intCpu), minCpu, maxCpu)
			if !isUpdate {
				continue
			}
			formatV[mid][port][def.CGROUP_TASK_CPU_SET] = cpuSet
		}
	}
	for mid, port := range mPorts {
		_ = ReleaseShareCpus(mid, port)
	}
	for mid, v := range formatV {
		ext, _ := jsoniter.MarshalToString(v)
		task := &tbl_task.Task{
			Type:      def.TASK_TYPE_CGROUP,
			Status:    def.TASK_NEW,
			MachineId: mid,
		}
		task.TaskExt.ExtString = ext
		task.TaskExt.Operation = def.OPERATION_CGROUP_APPLY
		err := tbl_task.CreateTask(task)
		if err != nil {
			log.Errorf("create cgroup task faild machineId:%d ext: %s err:%+v", mid, ext, err)
			return err
		}
	}
	return nil
}

func GeneralShareCpuTask(resourceList []*tbl_resource_pool.Resource) {
	//[machineid][servicePort][cpu_set]=>string value
	formatV := make(map[uint]map[uint]map[string]string, 0)
	//mid:port
	mports := make(map[uint][]uint, 0)
	for _, list := range resourceList {
		nodes, err := tbl_node.GetOnlineClusterMachine(list.ClusterId, list.ServiceId, false)
		if err != nil {
			log.Errorf("apply failed clusterId:%d serviceId:%d err:%+v", list.ClusterId, list.ServiceId, err)
			continue
		}
		machineIds := make([]uint, 0)
		for _, node := range nodes {
			machineIds = append(machineIds, node.MachineId)
		}
		machineInfos, err := tbl_machine.GetList(machineIds)
		if err != nil {
			log.Errorf("get machineInfos:%v faild err:%v", machineIds, err)
		}
		delMids := make(map[uint]int, 0)
		machineVal := make(map[uint]string)
		if len(machineInfos) > 0 {
			for _, machineInfo := range machineInfos {
				if machineInfo.IDC != list.IDC {
					delMids[machineInfo.ID] = 1
				}
				share := fmt.Sprintf("%d-%d", machineInfo.CpuSetMax+1, machineInfo.CpuTotal-1)
				machineVal[machineInfo.ID] = share
			}
		}
		for _, node := range nodes {
			if _, ok := delMids[node.MachineId]; ok {
				continue
			}
			if formatV[node.MachineId] == nil {
				formatV[node.MachineId] = make(map[uint]map[string]string, 0)
			}
			if mports[node.MachineId] == nil {
				mports[node.MachineId] = make([]uint, 0)
			}
			if formatV[node.MachineId][node.ServicePort] == nil {
				formatV[node.MachineId][node.ServicePort] = make(map[string]string, 0)
			}
			mports[node.MachineId] = append(mports[node.MachineId], node.ServicePort)
			if _, ok := machineVal[node.MachineId]; ok {
				formatV[node.MachineId][node.ServicePort][def.CGROUP_TASK_CPU_SHARE] = machineVal[node.MachineId]
			}
		}
		resource := &tbl_resource_pool.Resource{
			ApplyTime: time.Now().Unix(),
		}
		err = tbl_resource_pool.Update(list.ID, resource)
		if err != nil {
			log.Warnf("update applytime fail, err:%v", err)
		}
	}
	for mid, v := range formatV {
		ext, _ := jsoniter.MarshalToString(v)
		task := &tbl_task.Task{
			Type:      def.TASK_TYPE_CGROUP,
			Status:    def.TASK_NEW,
			MachineId: mid,
		}
		task.TaskExt.ExtString = ext
		task.TaskExt.Operation = def.OPERATION_CGROUP_SHARE_CPUS
		err := tbl_task.CreateTask(task)
		if err != nil {
			log.Errorf("create task faild machineId:%d err:%+v", mid, err)
			continue
		}
		if _, ok := mports[mid]; ok {
			_ = ChangeCpuSetType(mid, mports[mid], def.SHARE_CPU)
		}
	}
}

func AssignCpuSet(machineId, servicePort uint, cpuNum, minCpu, maxCpu int) (string, bool) {
	if cpuNum <= 0 {
		log.Infof("machineId:%d port:%d cpuNum:%d no need assign cpu", machineId, servicePort, cpuNum)
		return "", false
	}
	machineInfo, err := tbl_machine.GetInfoUseMaster(machineId)
	if err != nil {
		log.Errorf("get machine:%d info failed, err: %v", machineId, err)
		return "", false
	}
	var r string
	needNums := cpuNum
	if len(machineInfo.CpuSet) <= 0 {
		var cpus string
		for i := minCpu; i < cpuNum; i++ {
			strI := strconv.FormatInt(int64(i), 10)
			cpus += strI + ","
		}
		cpus = strings.TrimRight(cpus, ",")
		log.Infof("machineId=%d port=%d cpuNum=%d no set cpu, cpus=%s", machineId, servicePort, cpuNum, cpus)
		_ = updateCpuSet(nil, cpus, machineId, servicePort)
		return cpus, true
	}
	cpuSet := make(map[uint]string, 0)
	_ = jsoniter.UnmarshalFromString(machineInfo.CpuSet, &cpuSet)
	cpus := make(map[int64]int, 0)
	for port, cpu := range cpuSet {
		if len(cpu) == 0 {
			continue
		}
		f := strings.Split(cpu, ",")
		if port == servicePort {
			if len(f) > cpuNum {
				tmp := f[0:cpuNum]
				for _, t := range tmp {
					r += t + ","
				}
				r = strings.TrimRight(r, ",")
				log.Infof("machineId=%d port=%d cpuNum=%d reduce cpu, cpus=%s", machineId, servicePort, cpuNum, r)
				_ = updateCpuSet(cpuSet, r, machineId, servicePort)
				return r, true
			}
			if len(f) == cpuNum {
				return cpu, false
			}
			if len(f) < cpuNum {
				needNums = cpuNum - len(f)
			}
		}
		for _, i := range f {
			l, _ := strconv.ParseInt(i, 10, 64)
			cpus[l] = 1
		}
	}
	var n int
	cpuSetMax := machineInfo.CpuSetMax
	if maxCpu > 0 {
		cpuSetMax = maxCpu
	}
	if maxCpu > 0 || minCpu > 0 {
		for i := minCpu; i <= cpuSetMax; i++ {
			if n >= needNums {
				break
			}
			if _, ok := cpus[int64(i)]; ok {
				continue
			}
			n++
			strI := strconv.FormatInt(int64(i), 10)
			r += strI + ","
		}
		if n < needNums {
			log.Infof("machineId=%d port=%d need reassign", machineId, servicePort)
			n = 0
		}
	}

	for i := 0; i <= cpuSetMax; i++ {
		if n >= needNums {
			break
		}
		if _, ok := cpus[int64(i)]; ok {
			continue
		}
		n++
		strI := strconv.FormatInt(int64(i), 10)
		r += strI + ","
	}
	log.Infof("machineId=%d port=%d cpuNum=%d add cpu num=%d, cpus=%s", machineId, servicePort, cpuNum, needNums, r)
	if originCpu, ok := cpuSet[servicePort]; ok {
		r = originCpu + "," + r
	}
	r = strings.TrimRight(r, ",")
	_ = updateCpuSet(cpuSet, r, machineId, servicePort)
	return r, true
}

func ChangeCpuSetType(machineId uint, servicePorts []uint, cpuSetType int) error {
	machineInfo, err := tbl_machine.GetInfo(machineId)
	if err != nil {
		log.Errorf("get machine:%d info failed, err: %v", machineId, err)
		return err
	}
	shareK := fmt.Sprintf("%d-%d", machineInfo.CpuSetMax+1, machineInfo.CpuTotal-1)
	var strCpuSet string
	var strShareCpuSet string
	if cpuSetType == def.NOT_SET_CPU {
		if len(machineInfo.CpuSet) > 0 {
			cpuSet := make(map[uint]string, 0)
			_ = jsoniter.UnmarshalFromString(machineInfo.CpuSet, &cpuSet)
			for _, port := range servicePorts {
				if _, ok := cpuSet[port]; ok {
					delete(cpuSet, port)
				}
			}
			strCpuSet, _ = jsoniter.MarshalToString(cpuSet)
		}
		if len(machineInfo.ShareCpuSet) > 0 {
			shareCpu := make(map[string]map[uint]int, 0)
			_ = jsoniter.UnmarshalFromString(machineInfo.ShareCpuSet, &shareCpu)
			for _, port := range servicePorts {
				if _, ok := shareCpu[shareK]; ok {
					if _, ok := shareCpu[shareK][port]; ok {
						delete(shareCpu[shareK], port)
					}
				}
			}
			strShareCpuSet, _ = jsoniter.MarshalToString(shareCpu)
		}
		//update
		updateErr := tbl_machine.Update(machineId, tbl_machine.Machine{
			ShareCpuSet: strShareCpuSet,
			CpuSet:      strCpuSet,
		})
		if updateErr != nil {
			log.Errorf("update machine:%d sharecpuset failed, err=%v", machineId, updateErr)
		}
	}
	if cpuSetType == def.SHARE_CPU {
		if len(machineInfo.CpuSet) > 0 {
			cpuSet := make(map[uint]string, 0)
			_ = jsoniter.UnmarshalFromString(machineInfo.CpuSet, &cpuSet)
			for _, port := range servicePorts {
				if _, ok := cpuSet[port]; ok {
					delete(cpuSet, port)
				}
			}
			//update
			strCpuSet, _ = jsoniter.MarshalToString(cpuSet)
		}
		shareCpu := make(map[string]map[uint]int, 0)
		if len(machineInfo.ShareCpuSet) > 0 {
			shareCpu = make(map[string]map[uint]int, 0)
			_ = jsoniter.UnmarshalFromString(machineInfo.ShareCpuSet, &shareCpu)
			for _, port := range servicePorts {
				if _, ok := shareCpu[shareK]; ok {
					if _, ok := shareCpu[shareK][port]; !ok {
						shareCpu[shareK][port] = 1
					}
				}
			}
		} else {
			shareCpu[shareK] = make(map[uint]int, 0)
			for _, port := range servicePorts {
				shareCpu[shareK][port] = 1
			}
		}
		if len(shareCpu[shareK]) > 0 {
			strShareCpuSet, _ = jsoniter.MarshalToString(shareCpu)
		}
		updateErr := tbl_machine.Update(machineId, tbl_machine.Machine{
			ShareCpuSet: strShareCpuSet,
			CpuSet:      strCpuSet,
		})
		if updateErr != nil {
			log.Errorf("update machine:%d sharecpuset failed, err=%v", machineId, updateErr)
		}
	}
	return nil
}

func ReleaseShareCpus(machineId uint, servicePorts []uint) error {
	machineInfo, err := tbl_machine.GetInfo(machineId)
	if err != nil {
		log.Errorf("get machine:%d info failed, err: %v", machineId, err)
		return err
	}
	shareK := fmt.Sprintf("%d-%d", machineInfo.CpuSetMax+1, machineInfo.CpuTotal-1)
	if len(machineInfo.ShareCpuSet) > 0 {
		shareCpu := make(map[string]map[uint]int, 0)
		_ = jsoniter.UnmarshalFromString(machineInfo.ShareCpuSet, &shareCpu)
		for _, port := range servicePorts {
			if _, ok := shareCpu[shareK]; ok {
				if _, ok := shareCpu[shareK][port]; ok {
					delete(shareCpu[shareK], port)
				}
			}
		}
		strShareCpu, _ := jsoniter.MarshalToString(shareCpu)
		updateErr := tbl_machine.Update(machineId, tbl_machine.Machine{
			ShareCpuSet: strShareCpu,
		})
		if updateErr != nil {
			log.Errorf("update machine:%d sharecpuset failed, err=%v", machineId, updateErr)
		}
	}
	return nil
}

func updateCpuSet(oriCpuSet map[uint]string, newCpu string, machineId, port uint) error {
	if oriCpuSet == nil {
		oriCpuSet = make(map[uint]string, 0)
	}
	oriCpuSet[port] = newCpu
	strCpuSet, _ := jsoniter.MarshalToString(oriCpuSet)
	//update
	updateErr := tbl_machine.Update(machineId, tbl_machine.Machine{
		CpuSet: strCpuSet,
	})
	if updateErr != nil {
		log.Errorf("update machine cpuset failed, err=%v", updateErr)
	}
	return updateErr
}
