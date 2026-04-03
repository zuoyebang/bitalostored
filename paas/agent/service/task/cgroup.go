package task

import (
	"bufio"
	"fmt"
	tbl_machine "github.com/zuoyebang/bitalostored/paas/agent/dao/machine"
	tbl_node "github.com/zuoyebang/bitalostored/paas/agent/dao/node"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/config"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/def"
	log "github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/webclient"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

func SetCgroup(machineInfo *tbl_machine.Machine) {
	log.Info("start set cgroup")
	cmd := fmt.Sprintf("ps -ef |grep 'config/supervisor.conf' |grep '%s' |awk '{print $8}' > cmd.tmp", "homework/bitalos-paas/bitalos-data")
	_, _ = exec.Command("bash", "-c", cmd).Output()
	f, err := os.Open("cmd.tmp")
	if err != nil {
		log.Warnf("exec cmd failed, err=%v", err)
		return
	}
	defer func() {
		f.Close()
		os.Remove("cmd.tmp")
	}()

	cpuSetInfo := make(map[string]string, 0)                 // {"8080":"1,2,3",...}
	cpuSetShareInfoStr := make(map[string]map[string]int, 0) // {"90-95":{"8080":1,...}}
	var cpuSetShareInfo map[string]int
	if len(machineInfo.CpuSet) > 0 {
		jsoniter.Unmarshal([]byte(machineInfo.CpuSet), &cpuSetInfo)
	}
	if len(machineInfo.ShareCpuSet) > 0 {
		jsoniter.Unmarshal([]byte(machineInfo.ShareCpuSet), &cpuSetShareInfoStr)
		for c, shareInfo := range cpuSetShareInfoStr {
			if len(c) > 0 {
				cpuSetShareInfo = shareInfo
				break
			}
		}
	}
	log.Infof("decode cpuset info. cpuset: %+v, cpuShareSet: %+v", cpuSetInfo, cpuSetShareInfo)

	shareProcess := 1
	excludeProcess := 2
	unknownProcess := 3

	checkPortShare := func(port string) int {
		if _, ok := cpuSetInfo[port]; ok {
			return excludeProcess
		}
		if _, ok := cpuSetShareInfo[port]; ok {
			return shareProcess
		}
		return unknownProcess
	}

	writePidToCpuset := func(serviceType string, clusterName string, port string, cgroupPid string, portStatus int) error {
		if portStatus != excludeProcess && portStatus != shareProcess {
			return nil
		}
		if serviceType != def.CGROUP_SERVER_NAME && serviceType != def.CGROUP_PROXY_NAME {
			return nil
		}

		cpusetType := 0
		cpuSetRoot := ""
		if portStatus == excludeProcess {
			cpuSetRoot = def.CGROUOP_CPU_SET_PATH
			cpusetType = def.CPUSET_EXCLUSIVE
		} else if portStatus == shareProcess {
			cpuSetRoot = def.CGROUP_CPU_SHARE_PATH
			cpusetType = def.CPUSET_SHARE
		}

		cpuSetDir := fmt.Sprintf("%s/%s_%s_%s", cpuSetRoot, serviceType, clusterName, port)
		if err = createCgroupDir(cpuSetDir); err != nil {
			return err
		}
		if err = WriteCgroupCpuSetConfig(cpuSetDir, cgroupPid, "", cpusetType); err != nil {
			return err
		}
		return nil
	}
	writePidToCpu := func(serviceType string, clusterName string, port string, cgroupPid string) error {
		if serviceType != def.CGROUP_SERVER_NAME && serviceType != def.CGROUP_PROXY_NAME {
			return nil
		}
		cgroupDir := fmt.Sprintf("%s/%s_%s_%s", def.CGROUP_CPU_PATH, serviceType, clusterName, port)
		if err := createCgroupDir(cgroupDir); err != nil {
			return err
		}
		if err := WriteCgroupPid(cgroupDir, cgroupPid); err != nil {
			return err
		}
		return nil
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, "/")
		if len(arr) <= 5 {
			continue
		}
		if strings.Contains(line, "stored-fe") || strings.Contains(line, "stored-dashboard") {
			continue
		}
		processDir := line[0 : len(line)-len("/bin/supervisord")]
		if strings.Contains(line, "stored-proxy") { //proxy
			clusterName := arr[6]
			nodeSp := arr[7]
			nodeArr := strings.Split(nodeSp, "-")
			if len(nodeArr) != 3 {
				continue
			}
			port := nodeArr[len(nodeArr)-1]
			cgroupPid := getPid(processDir, def.SERVICE_PROXY)
			writePidToCpu(def.CGROUP_PROXY_NAME, clusterName, port, cgroupPid)
			portStatus := checkPortShare(port)
			writePidToCpuset(def.CGROUP_PROXY_NAME, clusterName, port, cgroupPid, portStatus)
		} else {
			clusterName := arr[7]
			nodeSp := arr[9]
			nodeArr := strings.Split(nodeSp, "-")
			if len(nodeArr) != 4 {
				continue
			}
			port := nodeArr[len(nodeArr)-1]
			cgroupPid := getPid(processDir, def.SERVICE_BITALOS)
			writePidToCpu(def.CGROUP_SERVER_NAME, clusterName, port, cgroupPid)
			portStatus := checkPortShare(port)
			writePidToCpuset(def.CGROUP_SERVER_NAME, clusterName, port, cgroupPid, portStatus)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Warnf("scan file err:%v", err)
	}
}

func ApplyCgroup(task *TaskInfo) {
	cpuFiles, err := os.ReadDir(def.CGROUP_CPU_PATH)
	if err != nil {
		log.Errorf("read cpu dir fail, err:%v", err)
		return
	}
	portDir := make(map[int64]string, 0)
	portType := make(map[int64]string, 0)
	for _, file := range cpuFiles {
		if file.IsDir() {
			spl := strings.Split(file.Name(), "_")
			port := spl[len(spl)-1]
			intPort, _ := strconv.ParseInt(port, 10, 64)
			portDir[intPort] = file.Name()
			portType[intPort] = spl[0]
		}
	}
	ext := task.TaskExt.ExtString
	if task.TaskExt.Operation == def.TASK_OPERATION_APPLY_CGROUP {
		cgroupTask := make(map[int64]map[string]string, 0)
		_ = jsoniter.UnmarshalFromString(ext, &cgroupTask)
		for servicePort, v := range cgroupTask {
			if dirName, ok := portDir[servicePort]; ok {
				pid := getCgroupPid(servicePort, portType[servicePort])
				if _, ok := v[def.CGROUP_TASK_CPU]; ok {
					intCpu, _ := strconv.ParseInt(v[def.CGROUP_TASK_CPU], 10, 64)
					quotaUs := def.CGROUP_DEFAULT_PERIOD_US * int(intCpu)
					cgroupDir := path.Join(def.CGROUP_CPU_PATH, dirName)
					err = WriteCgroupCpuQuota(cgroupDir, quotaUs)
					if err != nil {
						continue
					}
					err = WriteCgroupCpuPeriod(cgroupDir, def.CGROUP_DEFAULT_PERIOD_US)
					if err != nil {
						continue
					}
					_ = WriteCgroupPid(cgroupDir, pid)
				}
				if cpus, ok := v[def.CGROUP_TASK_CPU_SET]; ok {
					cgroupDir := path.Join(def.CGROUOP_CPU_SET_PATH, dirName)
					err = createCgroupDir(cgroupDir)
					if err != nil {
						continue
					}
					err = WriteCgroupCpuSetConfig(cgroupDir, pid, cpus, 1)
					if err != nil {
						continue
					}
				}
				shareCpuDir := path.Join(def.CGROUP_CPU_SHARE_PATH, dirName)
				_ = rmCgroupDir(shareCpuDir)
			}
		}
	}
	if task.TaskExt.Operation == def.TASK_OPERATION_RELEASE_CPUS {
		if len(ext) > 0 {
			var ports []int64
			_ = jsoniter.UnmarshalFromString(ext, &ports)
			if len(ports) > 0 {
				for _, port := range ports {
					if dirName, ok := portDir[port]; ok {
						cpuSetDir := path.Join(def.CGROUOP_CPU_SET_PATH, dirName)
						ReleaseCpuSetPid(cpuSetDir)
						//rmdir share
						shareCpuDir := path.Join(def.CGROUP_CPU_SHARE_PATH, dirName)
						ReleaseCpuSetPid(shareCpuDir)
					}
				}
			}
		}
	}
	if task.TaskExt.Operation == def.TASK_OPERATION_SHARE_CPUS {
		if len(ext) > 0 {
			cgroupTask := make(map[int64]map[string]string, 0)
			_ = jsoniter.UnmarshalFromString(ext, &cgroupTask)
			for servicePort, v := range cgroupTask {
				if dirName, ok := portDir[servicePort]; ok {
					pid := getCgroupPid(servicePort, portType[servicePort])
					if share, ok := v[def.CGROUP_TASK_CPU_SHARE]; ok {
						shareDir := path.Join(def.CGROUP_CPU_SHARE_PATH, dirName)
						cpuSetDir := path.Join(def.CGROUOP_CPU_SET_PATH, dirName)
						err = createCgroupDir(shareDir)
						if err != nil {
							continue
						}
						err = WriteCgroupCpuSetConfig(shareDir, pid, share, 0)
						if err != nil {
							continue
						}
						_ = rmCgroupDir(cpuSetDir)
					}
				}
			}
		}
	}
	webclient.PostPaaS(config.GetPaaSAddress(def.URL_STATUS), map[string]interface{}{
		"taskId": task.TaskId, "taskIds": []int64{task.TaskId}, "taskStatus": "success", "errors": "",
	})
}

func ReleaseCpuSetPid(cpuDir string) {
	_, err := os.Stat(cpuDir)
	if err == nil {
		cmd := fmt.Sprintf("cat %s", path.Join(cpuDir, "cgroup.procs"))
		pid, _ := exec.Command("bash", "-c", cmd).Output()
		strPid := strings.Replace(string(pid), "\n", "", -1)
		log.Infof("exec cmd:%s result=%s", cmd, strPid)
		if len(strPid) > 0 {
			filename := path.Join(def.CGROUOP_CPU_SET_PATH, "cgroup.procs")
			cmd = fmt.Sprintf("echo %s > %s", strPid, filename)
			_, err = exec.Command("bash", "-c", cmd).Output()
			if err != nil {
				log.Warnf("cmd: %s err, err = %v", cmd, err)
			} else {
				log.Infof("cmd: %s success", cmd)
			}
		}
		//rmdir dir
		_ = rmCgroupDir(cpuDir)
	}
}

func WriteCgroupCpuConfig(cgroupPid, cgroupDir, serviceName string, cpuNums int) error {
	periodUs := def.CGROUP_DEFAULT_PERIOD_US
	quotaUs := def.CGROUP_DEFAULT_SERVER_QUOTA_US
	if serviceName == def.SERVICE_PROXY {
		quotaUs = def.CGROUP_DEFAULT_PROXY_QUOTA_US
	}
	if cpuNums > 0 {
		quotaUs = cpuNums * periodUs
	}
	err := WriteCgroupPid(cgroupDir, cgroupPid)
	if err != nil {
		return err
	}
	err = WriteCgroupCpuPeriod(cgroupDir, periodUs)
	if err != nil {
		return err
	}
	err = WriteCgroupCpuQuota(cgroupDir, quotaUs)
	if err != nil {
		return err
	}
	return nil
}

func WriteCgroupCpuQuota(dir string, quotaUs int) error {
	filename := path.Join(dir, "cpu.cfs_quota_us")
	cmd := fmt.Sprintf("echo %d > %s", quotaUs, filename)
	_, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		log.Warnf("cmd: %s err, err = %v", cmd, err)
		return err
	}
	log.Infof("cmd: %s success", cmd)
	return nil
}

func WriteCgroupCpuPeriod(dir string, periodUs int) error {
	filename := path.Join(dir, "cpu.cfs_period_us")
	cmd := fmt.Sprintf("echo %d > %s", periodUs, filename)
	_, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		log.Warnf("cmd: %s err, err = %v", cmd, err)
		return err
	}
	log.Infof("cmd: %s success", cmd)
	return nil
}

func getCgroupPid(port int64, processType string) string {
	var condition string
	if processType == "proxy" {
		condition = fmt.Sprintf("%d/bin/stored-proxy", port)
	}
	if processType == "server" {
		condition = fmt.Sprintf("%d/bin/stored-bitalos", port)
	}
	cmd := "ps -ef |grep " + condition + " | grep -v grep |awk '{print $2}'"
	pid, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		log.Errorf("grep pid err, err = %v", err)
		return ""
	}
	return string(pid)
}

func WriteCgroupCpuSetConfig(cgroupDir, pid, cpus string, exclusive int) error {
	_ = WriteCgroupCpuSetMems(cgroupDir, 0, false)
	_ = WriteCgroupCpuSetExclusive(cgroupDir, exclusive, false)
	_ = WriteCgroupCpuSetCpus(cgroupDir, cpus)
	_ = WriteCgroupPid(cgroupDir, pid)
	return nil
}

func WriteCgroupCpuSetMems(dir string, mems int, checkFileEmpty bool) error {
	filename := path.Join(dir, "cpuset.mems")
	if checkFileEmpty {
		if !checkFileContentEmpty(filename) {
			return nil
		}
	}

	cmd := fmt.Sprintf("echo %d > %s", mems, filename)
	_, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		log.Warnf("cmd: %s err, err = %v", cmd, err)
		return err
	}
	log.Infof("cmd: %s success", cmd)
	return nil
}

func GetMachineInfo() *tbl_machine.Machine {
	if machineInfo, err := tbl_machine.GetMachinesByIp(config.C.IP); err != nil || (machineInfo != nil && machineInfo.ID <= 0) {
		log.Warnf("get machineInfo error. ip=%s err=%v", config.C.IP, err)
		panic("get machine error")
	} else {
		if len(machineInfo.HostName) == 0 {
			hostname, err := os.Hostname()
			if err != nil {
				log.Warnf("get hostname err")
			} else {
				err = tbl_machine.UpdateHostname(machineInfo.ID, hostname)
				if err != nil {
					log.Warnf("update hostname err:%s. hostname:%s", err, hostname)
				}
			}
		}
		return machineInfo
	}
}

func UpdateMachineTime(machineInfo *tbl_machine.Machine) error {
	if machineInfo == nil {
		return nil
	}
	err := tbl_machine.UpdateTime(machineInfo.ID, tbl_machine.Machine{})
	log.Infof("mid %d ip %s update machine time err %v", machineInfo.ID, config.C.IP, err)
	return err
}

func InitRootCpuSet(machineInfo *tbl_machine.Machine) {
	if machineInfo == nil {
		return
	}
	var cpuSetMax, cpuTotal int
	cpuTotal = machineInfo.CpuTotal
	cpuSetMax = machineInfo.CpuSetMax

	// {shareCpu=cpuTotal-(cpuSetMax+1)}>=1
	if cpuTotal == 0 || cpuSetMax == 0 || cpuTotal-(cpuSetMax+1) < 0 {
		log.Warnf("cpu settting error(cpusetmax<=cputotal-2), check db. cpuSetMax=%d cpuTotal=%d", cpuSetMax, cpuTotal)
		return
	}

	cpusetDir := def.CGROUOP_CPU_SET_PATH
	WriteCgroupCpuSetMems(cpusetDir, 0, true)
	initRootCpuSetCpus(cpusetDir, 0, cpuTotal-1)
	WriteCgroupCpuSetExclusive(cpusetDir, 1, false)

	shareDir := def.CGROUP_CPU_SHARE_PATH
	if err := createCgroupDir(shareDir); err != nil {
		return
	}
	WriteCgroupCpuSetMems(shareDir, 0, true)
	initRootCpuSetCpus(shareDir, cpuSetMax+1, cpuTotal-1)
	WriteCgroupCpuSetExclusive(shareDir, 0, true)
}

func initRootCpuSetCpus(cpusetDir string, minCpuId, maxCpuId int) error {
	file := filepath.Join(cpusetDir, "cpuset.cpus")
	if !checkFileContentEmpty(file) {
		return nil
	}
	cpusetCpusContent := fmt.Sprintf("%d-%d", minCpuId, maxCpuId)
	return WriteCgroupContent(file, cpusetCpusContent)
}

// empty=true, noempty=false
func checkFileContentEmpty(filename string) bool {
	fd, err := os.OpenFile(filename, os.O_RDONLY, 0444)
	if err != nil {
		return true
	}
	defer fd.Close()

	reader := bufio.NewReader(fd)
	line, _, _ := reader.ReadLine()
	if len(line) <= 0 {
		return true
	}
	return false
}

func WriteCgroupContent(file string, content string) error {
	cmd := fmt.Sprintf("echo %s > %s", content, file)
	_, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		log.Warnf("cmd: %s err, err = %v", cmd, err)
		return err
	}
	log.Infof("cmd: %s success", cmd)
	return nil
}

func WriteCgroupCpuSetExclusive(dir string, exclusive int, checkFileEmpty bool) error {
	filename := path.Join(dir, "cpuset.cpu_exclusive")
	if checkFileEmpty && !checkFileContentEmpty(filename) {
		return nil
	}

	cmd := fmt.Sprintf("echo %d > %s", exclusive, filename)
	_, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		log.Warnf("cmd: %s err, err = %v", cmd, err)
		return err
	}
	log.Infof("cmd: %s success", cmd)
	return nil
}

func WriteCgroupCpuSetCpus(dir, cpus string) error {
	if len(cpus) > 0 {
		filename := path.Join(dir, "cpuset.cpus")
		cmd := fmt.Sprintf("echo %s > %s", cpus, filename)
		_, err := exec.Command("bash", "-c", cmd).Output()
		if err != nil {
			log.Warnf("cmd: %s err, err = %v", cmd, err)
			return err
		}
		log.Infof("cmd: %s success", cmd)
	}
	return nil
}

func WriteCgroupPid(dir, pid string) error {
	if len(pid) > 0 {
		strPid := strings.Replace(pid, "\n", "", -1)
		filename := path.Join(dir, "cgroup.procs")
		cmd := fmt.Sprintf("echo %s > %s", strPid, filename)
		_, err := exec.Command("bash", "-c", cmd).Output()
		if err != nil {
			log.Warnf("cmd: %s err, err = %v", cmd, err)
			return err
		}
		log.Infof("cmd: %s success", cmd)
	}
	return nil
}

func ScanCpuStat(machineId uint) {
	go func(machineId uint) {
		for {
			time.Sleep(5 * time.Minute)
			//time.Sleep(10 * time.Second)
			cgroupDir := "/sys/fs/cgroup/cpu/stored"
			_, err := os.Stat(cgroupDir)
			if os.IsNotExist(err) {
				log.Warn("cgroup dir not exist")
				continue
			}
			dirEntries, err := os.ReadDir(cgroupDir)
			if err != nil {
				log.Warnf("cgroup dir read fail:%v", err)
				continue
			}

			cpuThrottled := make(map[string]string, len(dirEntries))
			for _, dirEntry := range dirEntries {
				if dirEntry.IsDir() {
					dirName := dirEntry.Name()
					dirSp := strings.Split(dirName, "_")
					if len(dirSp) > 1 {
						port := dirSp[len(dirSp)-1]
						cpuStatFile := path.Join(cgroupDir, dirName, "cpu.stat")
						cpuThrottled[port] = getThrottled(cpuStatFile)
					}
				}
			}
			_ = tbl_node.UpdateCpuThrottled(machineId, cpuThrottled)
		}
	}(machineId)
}

func getThrottled(filePath string) string {
	content, err := os.ReadFile(filePath)
	if err != nil {
		log.Errorf("read file:%s err:%v", filePath, err)
		return ""
	}

	lines := strings.Split(string(content), "\n")

	for _, line := range lines {
		if strings.HasPrefix(line, "nr_throttled") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				nrThrottled := fields[1]
				if nrThrottled == "0" {
					continue
				}
				log.Infof("file:%s cpuThrottled:%s", filePath, nrThrottled)
				return nrThrottled
			}
			break
		}
	}
	return ""
}
