package task

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/config"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/def"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/webclient"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

func GetList() ([]TaskInfo, error) {
	p := fmt.Sprintf("%s?machineId=%d", config.GetPaaSAddress(def.URL_LIST), config.C.MachineId)
	netClient := &http.Client{
		Timeout: 10 * time.Second,
	}
	res, err := netClient.Get(p)
	if err != nil {
		logs.Warn("getTaskList", err)
		return nil, err
	}

	resp := &TaskResp{}
	if err = json.NewDecoder(res.Body).Decode(resp); err != nil {
		logs.Warn("getTaskList", err, p)
		return nil, err
	}

	if len(resp.Data) > 0 {
		logs.Info("get task count: ", len(resp.Data))
	}
	for i := range resp.Data {
		task := resp.Data[i]
		logs.Info("run:", task.TaskId, "type:", task.TaskType)
		switch task.TaskType {
		case def.TYPE_PREPARE_START, def.TYPE_PREPARE_ADD:
			task.Prepare()
		case def.TYPE_UPGRADE:
			task.postStatus(config.GetPaaSAddress(def.URL_UPGRADED))
		case def.TYPE_START, def.TYPE_ADD, def.TYPE_OPERATE:
			task.postStatus(config.GetPaaSAddress(def.URL_STATUS))
		case def.TYPE_CGROUP:
			ApplyCgroup(&task)
		case def.TYPE_LINK:
			ApplyLink(&task)
		}
	}
	return resp.Data, nil
}

func (task *TaskInfo) Prepare() error {
	servicePort := GetServicePort(task.TaskExt.ServicePortRange[0], task.TaskExt.ServicePortRange[1])
	clusterPort := GetServicePort(task.TaskExt.ClusterPortRange[0], task.TaskExt.ClusterPortRange[1])

	logs.Info("prepare ", task.TaskId, "servicePort ", servicePort, "clusterPort ", clusterPort)
	_, err := webclient.PostPaaS(config.GetPaaSAddress(def.URL_PREPARED), map[string]interface{}{
		"taskId": task.TaskId, "nodeId": task.NodeId,
		"servicePort": servicePort, "clusterPort": clusterPort,
	})
	return err
}

func (task *TaskInfo) postStatus(url string) {
	err := task.RenderAndRun()
	if err != nil {
		webclient.PostPaaS(url, map[string]interface{}{
			"taskId": task.TaskId, "taskIds": []int64{task.TaskId}, "taskStatus": "fail", "errors": "",
		})
	} else {
		webclient.PostPaaS(url, map[string]interface{}{
			"taskId": task.TaskId, "taskIds": []int64{task.TaskId}, "taskStatus": "success", "errors": "",
		})
	}
}

type TaskResp struct {
	webclient.PaaSResponse
	Data []TaskInfo `json:"data"`
}

func (task *TaskInfo) RenderAndRun() error {
	taskLog, _ := json.Marshal(task)
	logs.Info("taskInfo:", string(taskLog))
	if e := task.PrepareFiles(); e != nil {
		logs.Warn("config:", e)
		return e
	}
	return task.Run(task.TaskPath)
}

func (task *TaskInfo) Run(dir string) error {
	if task.TaskExt.Operation == "" {
		logs.Info("no operation to do")
		return nil
	}

	if e := os.Chmod(path.Join(dir, "run.sh"), 0775); e != nil {
		logs.Warn("chmod", e)
	}

	logs.Info("task run")
	cmd := exec.Command(path.Join(dir, "run.sh"), task.TaskExt.Operation)

	logs.Info("command:", cmd)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	cmd.WaitDelay = 3 * time.Second

	oldPid := getPid(dir, task.TaskExt.ServiceName)
	logs.Infof("now pid:%s", oldPid)
	err := cmd.Run()
	if err != nil && !errors.Is(err, exec.ErrWaitDelay) {
		logs.Warnf("run:%v  error detail:%s", err, stderr.String())
		return err
	}
	logs.Info("exec command output:", out.String())

	logs.Info("task:", task.TaskId, "dir:", dir)

	if task.TaskExt.ServiceName != def.SERVICE_BITALOS && task.TaskExt.ServiceName != def.SERVICE_MATRIX &&
		task.TaskExt.ServiceName != def.SERVICE_PROXY {
		return nil
	}
	//cgroup
	cgroupCpuDir := getCgroupDir(task.TaskExt.ServiceName, task.TaskExt.ClusterName, task.TaskExt.ServicePort, def.CGROUP_CPU_PATH)
	cgroupCpuSetDir := getCgroupDir(task.TaskExt.ServiceName, task.TaskExt.ClusterName, task.TaskExt.ServicePort, def.CGROUOP_CPU_SET_PATH)
	cgroupShareCpuDir := getCgroupDir(task.TaskExt.ServiceName, task.TaskExt.ClusterName, task.TaskExt.ServicePort, def.CGROUP_CPU_SHARE_PATH)

	go func() {
		if task.TaskExt.Operation == "stop" || task.TaskExt.Operation == "upgrade" {
			time.Sleep(time.Second * 20)
			cgroupPid := getPid(dir, task.TaskExt.ServiceName)
			for i := 0; i < 30; i++ {
				if oldPid == cgroupPid {
					logs.Warnf("\"upgrade/stop pid: %s is old pid, this progress is stopping", oldPid)
					time.Sleep(time.Second * 60)
					cgroupPid = getPid(dir, task.TaskExt.ServiceName)
					continue
				}
				_, err := exec.Command("kill", "-0", cgroupPid).Output()
				if err != nil && err.Error() == "exit status 1" {
					logs.Warnf("upgrade/stop pid: %s not exists, wait 60s", cgroupPid)
					time.Sleep(time.Second * 60)
					cgroupPid = getPid(dir, task.TaskExt.ServiceName)
					continue
				}
				break
			}

			err := createCgroupDir(cgroupCpuDir)
			if err == nil {
				_ = WriteCgroupPid(cgroupCpuDir, cgroupPid)
			}
			err = createCgroupDir(cgroupCpuSetDir)
			if err == nil {
				_ = WriteCgroupCpuSetConfig(cgroupCpuSetDir, cgroupPid, "", 1)
			}
			err = createCgroupDir(cgroupShareCpuDir)
			if err == nil {
				_ = WriteCgroupCpuSetConfig(cgroupShareCpuDir, cgroupPid, "", 0)
			}
		}
		if task.TaskExt.Operation == "supervisor-stop" {
			for i := 0; i < 60; i++ {
				time.Sleep(time.Second * 10)
				err := rmCgroupDir(cgroupCpuDir)
				err1 := rmCgroupDir(cgroupCpuSetDir)
				err2 := rmCgroupDir(cgroupShareCpuDir)
				if err == nil {
					if err1 == nil || err2 == nil {
						break
					}
				}
			}
		}
		if task.TaskExt.Operation == "supervisor-start" {
			time.Sleep(time.Second * 10)
			cgroupPid := getPid(dir, task.TaskExt.ServiceName)
			var cpuNums int
			var cpuSet string
			var shareCpu string
			if len(task.TaskExt.ExtString) > 0 {
				cgroupTask := make(map[uint]map[string]string, 0)
				err := jsoniter.UnmarshalFromString(task.TaskExt.ExtString, &cgroupTask)
				if err == nil {
					for _, v := range cgroupTask {
						if _, ok := v[def.CGROUP_TASK_CPU]; ok {
							intCpuNums, _ := strconv.ParseInt(v["cpu"], 10, 64)
							cpuNums = int(intCpuNums)
						}
						if _, ok := v[def.CGROUP_TASK_CPU_SET]; ok {
							cpuSet = v[def.CGROUP_TASK_CPU_SET]
						}
						if _, ok := v[def.CGROUP_TASK_CPU_SHARE]; ok {
							shareCpu = v[def.CGROUP_TASK_CPU_SHARE]
						}
					}
				}
			}
			err := createCgroupDir(cgroupCpuDir)
			if err == nil {
				logs.Infof("cgroup pid:%s dir:%s serviceName:%s cpu:%d", cgroupPid, cgroupCpuDir, task.TaskExt.ServiceName, cpuNums)
				err = WriteCgroupCpuConfig(cgroupPid, cgroupCpuDir, task.TaskExt.ServiceName, cpuNums)
				if err != nil {
					return
				}
			}
			err = createCgroupDir(cgroupCpuSetDir)
			if err == nil {
				err = WriteCgroupCpuSetConfig(cgroupCpuSetDir, cgroupPid, cpuSet, 1)
				if err != nil {
					return
				}
			}
			err = createCgroupDir(cgroupShareCpuDir)
			if err == nil {
				err = WriteCgroupCpuSetConfig(cgroupShareCpuDir, cgroupPid, shareCpu, 0)
				if err != nil {
					return
				}
			}
		}
	}()
	return nil
}

func getPid(dir, serviceName string) string {
	if serviceName == def.SERVICE_PROXY {
		proxyFile := path.Join(dir, "stored-proxy.pid")
		_, err := os.Stat(proxyFile)
		if err != nil || os.IsNotExist(err) {
			logs.Warn("proxyFile:", proxyFile, " not exists")
			return ""
		}
		f, err := os.Open(proxyFile)
		if err != nil {
			logs.Warnf("read proxyFile err %v", err)
			return ""
		}
		defer f.Close()
		fd, err := ioutil.ReadAll(f)
		if err != nil {
			logs.Warnf("read proxyFile err %v", err)
			return ""
		}
		pid := string(fd)
		return pid
	}
	if serviceName == def.SERVICE_BITALOS || serviceName == def.SERVICE_MATRIX {
		supervisordFile := path.Join(dir, "supervisord-server.pid")
		_, err := os.Stat(supervisordFile)
		if err != nil || os.IsNotExist(err) {
			logs.Warn("supervisordFile:", supervisordFile, " not exists")
			return ""
		}
		f, err := os.Open(supervisordFile)
		if err != nil {
			logs.Warnf("read supervisordFile err %v", err)
			return ""
		}
		defer f.Close()
		fd, err := ioutil.ReadAll(f)
		if err != nil {
			logs.Warnf("read supervisordFile err %v", err)
			return ""
		}
		pid := string(fd)
		childId, err := exec.Command("pgrep", "-P", pid).Output()
		if err != nil {
			logs.Warnf("get child pid err, err = %v", err)
			return ""
		}
		ret := strings.Replace(string(childId), "\n", "", -1)
		return ret
	}
	return ""
}

func getCgroupDir(serviceName, clusterName string, servicePort int, parentDir string) string {
	var cgroupDir string
	if serviceName == def.SERVICE_BITALOS || serviceName == def.SERVICE_MATRIX {
		cgroupDir = fmt.Sprintf("%s/server_%s_%d", parentDir, clusterName, servicePort)
	}
	if serviceName == def.SERVICE_PROXY {
		cgroupDir = fmt.Sprintf("%s/proxy_%s_%d", parentDir, clusterName, servicePort)
	}
	return cgroupDir
}

func createCgroupDir(cgroupDir string) error {
	_, err := os.Stat(cgroupDir)
	if os.IsNotExist(err) {
		logs.Warn("cgroup dir:", cgroupDir, " not exists, start create")
		err = os.MkdirAll(cgroupDir, os.ModePerm)
		if err != nil {
			logs.Warnf("create dir: %s err %v", cgroupDir, err)
			return err
		}
		logs.Infof("cgroup dir %s create success", cgroupDir)
	}
	logs.Infof("cgroup dir %s exist", cgroupDir)
	return nil
}

func rmCgroupDir(cgroupDir string) error {
	_, err := os.Stat(cgroupDir)
	if err == nil {
		_, err = exec.Command("rmdir", cgroupDir).Output()
		if err != nil {
			logs.Warnf("rmdir dir:%s err, err = %v", cgroupDir, err)
			return err
		}
		logs.Infof("rmdir cgroup dir:%s succeed", cgroupDir)
	}
	return nil
}
