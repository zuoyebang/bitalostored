package service

import (
	"github.com/zuoyebang/bitalostored/paas/agent/internal/def"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"io/ioutil"
	"os/exec"
	"path"
	"strings"
)

func GetCgroups() map[string]interface{} {
	ret := make(map[string]interface{}, 0)
	ret["cpus"] = nil
	ret["cpuExclusive"] = nil
	ret["cpuShare"] = nil
	cpus := make(map[string]map[string]string, 0)
	cpuFiles, err := ioutil.ReadDir(def.CGROUP_CPU_PATH)
	if err != nil {
		logs.Errorf("read cpu dir:%s fail, err:%v", def.CGROUP_CPU_PATH, err)
		return ret
	}
	for _, file := range cpuFiles {
		if file.IsDir() {
			procsFile := path.Join(def.CGROUP_CPU_PATH, file.Name(), "cgroup.procs")
			procs := catFileContents(procsFile)
			if len(procs) <= 0 {
				continue
			}
			spl := strings.Split(file.Name(), "_")
			port := spl[len(spl)-1]
			if _, ok := cpus[port]; !ok {
				cpus[port] = make(map[string]string, 0)
			}
			file1 := path.Join(def.CGROUP_CPU_PATH, file.Name(), "cpu.cfs_period_us")
			periodUs := catFileContents(file1)
			cpus[port]["cfsPeriodUs"] = periodUs
			file2 := path.Join(def.CGROUP_CPU_PATH, file.Name(), "cpu.cfs_quota_us")
			quotaUs := catFileContents(file2)
			cpus[port]["cfsQuotaUs"] = quotaUs
		}
	}
	ret["cpus"] = cpus
	//cpuExclusive
	cpuExclusive := make(map[string]map[string]string, 0)
	cpuExclusiveFiles, err := ioutil.ReadDir(def.CGROUOP_CPU_SET_PATH)
	if err != nil {
		logs.Errorf("read cpu dir:%s fail, err:%v", def.CGROUOP_CPU_SET_PATH, err)
		return ret
	}
	for _, file := range cpuExclusiveFiles {
		if file.IsDir() {
			procsFile := path.Join(def.CGROUOP_CPU_SET_PATH, file.Name(), "cgroup.procs")
			procs := catFileContents(procsFile)
			if len(procs) <= 0 {
				continue
			}
			spl := strings.Split(file.Name(), "_")
			port := spl[len(spl)-1]
			if _, ok := cpuExclusive[port]; !ok {
				cpuExclusive[port] = make(map[string]string, 0)
			}
			exclusiveFile := path.Join(def.CGROUOP_CPU_SET_PATH, file.Name(), "cpuset.cpu_exclusive")
			exclusive := catFileContents(exclusiveFile)
			cpuExclusive[port]["cpuExclusive"] = exclusive
			memsFile := path.Join(def.CGROUOP_CPU_SET_PATH, file.Name(), "cpuset.mems")
			mems := catFileContents(memsFile)
			cpuExclusive[port]["cpuMems"] = mems
			cpusFile := path.Join(def.CGROUOP_CPU_SET_PATH, file.Name(), "cpuset.cpus")
			cpus := catFileContents(cpusFile)
			cpuExclusive[port]["cpus"] = cpus
		}
	}
	ret["cpuExclusive"] = cpuExclusive
	//cpuShare
	cpuShare := make(map[string]map[string]string, 0)
	cpuShareFiles, err := ioutil.ReadDir(def.CGROUP_CPU_SHARE_PATH)
	if err != nil {
		logs.Errorf("read cpu dir:%s fail, err:%v", def.CGROUP_CPU_SHARE_PATH, err)
		return ret
	}
	for _, file := range cpuShareFiles {
		if file.IsDir() {
			procsFile := path.Join(def.CGROUP_CPU_SHARE_PATH, file.Name(), "cgroup.procs")
			procs := catFileContents(procsFile)
			if len(procs) <= 0 {
				continue
			}
			spl := strings.Split(file.Name(), "_")
			port := spl[len(spl)-1]
			if _, ok := cpuShare[port]; !ok {
				cpuShare[port] = make(map[string]string, 0)
			}
			exclusiveFile := path.Join(def.CGROUP_CPU_SHARE_PATH, file.Name(), "cpuset.cpu_exclusive")
			exclusive := catFileContents(exclusiveFile)
			cpuShare[port]["cpuExclusive"] = exclusive
			memsFile := path.Join(def.CGROUP_CPU_SHARE_PATH, file.Name(), "cpuset.mems")
			mems := catFileContents(memsFile)
			cpuShare[port]["cpuMems"] = mems
			cpusFile := path.Join(def.CGROUP_CPU_SHARE_PATH, file.Name(), "cpuset.cpus")
			cpus := catFileContents(cpusFile)
			cpuShare[port]["cpus"] = cpus
		}
	}
	ret["cpuShare"] = cpuShare
	return ret
}

func catFileContents(file string) string {
	contents, err := exec.Command("cat", file).Output()
	if err != nil {
		logs.Warnf("cat file:%s err, err = %v", file, err)
		return ""
	}
	strC := string(contents)
	ret := strings.Replace(strC, "\n", "", -1)
	logs.Infof("cat file:%s contents:%s", file, ret)
	return ret
}
