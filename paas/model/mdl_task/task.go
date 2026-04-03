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

package mdl_task

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cluster"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_config"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_cosfile"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_group"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_service"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_dashboard"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_group"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_port"
	"github.com/zuoyebang/bitalostored/paas/model/mdl_resource_pool"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"github.com/zuoyebang/bitalostored/paas/utils/math2"
	"github.com/zuoyebang/bitalostored/paas/utils/net2"
	"github.com/zuoyebang/bitalostored/paas/utils/rpc"
	"path"
	"strconv"
	"strings"
	"text/template"
)

type Task struct {
	*tbl_task.Task
	FileType string `json:"fileType"`
	FileMode string `json:"fileMode"`
	FileName string `json:"fileName"`
	CosKey   string `json:"cosKey"`
	Content  string `json:"content"`

	TaskPath string `json:"taskPath"`
	TaskRoot string `json:"taskRoot"`

	TaskFiles []TaskFile `json:"taskFiles"`
}

type TaskFile struct {
	FileType string `json:"fileType"`
	FileMode string `json:"fileMode"`
	FileName string `json:"fileName"`
	CosKey   string `json:"cosKey"`
	Content  string `json:"content"`
}

func GetList(machineId uint, types []string, status []string, limit int, offset int) ([]*Task, error) {
	list, err := tbl_task.GetList(machineId, types, status, limit, offset)
	if err != nil {
		return nil, err
	}
	var res []*Task
	for _, task := range list {
		if tbl_task.AddNotCheckType(task.Type) {
			res = append(res, &Task{Task: task})
			continue
		}

		clusterInfo, err := tbl_cluster.GetInfo(task.ClusterId)
		if err != nil {
			log.Warn("get cluster info failed.err:", err)
			return res, err
		}
		configFiles, _ := tbl_config.GetListByPack(clusterInfo.ConfigPackId, clusterInfo.ServiceId)
		taskPath := ""
		if task.TaskExt.ServiceName == def.SERVICE_MATRIX || task.TaskExt.ServiceName == def.SERVICE_BITALOS {
			taskPath = path.Join(config.GetConf().Deploy.DeployPath, task.TaskExt.RegionName, task.TaskExt.ServiceName, task.TaskExt.ClusterName,
				fmt.Sprintf("group-%d/node-%d-port-%d", task.GroupId, task.NodeId, task.TaskExt.ServicePort))
		} else {
			taskPath = path.Join(config.GetConf().Deploy.DeployPath, task.TaskExt.ServiceName, task.TaskExt.ClusterName,
				fmt.Sprintf("/node-port-%d", task.TaskExt.ServicePort))
		}

		var files []TaskFile
		for _, cf := range configFiles {
			if !task.TaskExt.UpdateConfig {
				break
			}

			if task.ServiceId == def.SERVICE_ID_BITALOS || task.ServiceId == def.SERVICE_ID_MATRIX {
				if task.ServiceId != cf.ServiceId {
					continue
				}
			}

			content := cf.Content
			if cf.NeedRender {
				content, err = render(cf.Content, &Task{Task: task, TaskPath: taskPath, TaskRoot: "/home/homework/bitalos-paas//bitalos-agent"})
				if err != nil {
					log.Warn("render file failed.err:", err)
					return res, err
				}
				if shouldUpdateNodeconfig(task.TaskExt.ServiceName, task.TaskExt.CloudType, cf.Name) {
					err = tbl_node.Update(task.NodeId, task.GroupId, task.ClusterId, tbl_node.Node{ConfigContent: content})
					if err != nil {
						log.Warn("update node failed.err:", err)
					}
				}
			}
			files = append(files, TaskFile{
				FileType: cf.FileType,
				FileName: cf.Name,
				FileMode: cf.FileMode,
				Content:  content,
				CosKey:   "",
			})
		}
		cf, err := tbl_cosfile.GetCosFile(task.CosFileId)
		if err != nil {
			log.Warn("get cos files failed.err:", err)
			return res, err
		}
		if cf.CosKey == "" {
			log.Warn("cosKey is empty.cosFileId:", task.CosFileId)
		} else {
			files = append(files, TaskFile{
				FileType: cf.FileType,
				FileName: cf.Name,
				FileMode: cf.FileMode,
				Content:  "",
				CosKey:   cf.CosKey,
			})
		}
		if task.TaskExt.ServiceName == def.SERVICE_STORED_FE {
			cosFE, err := tbl_cosfile.GetCosFile(task.CosFileId + 1)
			if err != nil {
				log.Warn("get cos files failed.err:", err)
				return res, err
			}
			if cosFE.Name != "bitalosfe.tar.zz" || cosFE.Version != cf.Version {
				return res, errors.New("check build files please.could not find FE zip file")
			}
			files = append(files, TaskFile{
				FileType: cosFE.FileType,
				FileName: cosFE.Name,
				FileMode: cosFE.FileMode,
				Content:  "",
				CosKey:   cosFE.CosKey,
			})
		}
		if task.TaskExt.ServiceName == def.SERVICE_MATRIX || task.TaskExt.ServiceName == def.SERVICE_STORED_PROXY || task.TaskExt.ServiceName == def.SERVICE_BITALOS {
			if task.TaskExt.UpdateConfig {
				files = append(files, TaskFile{
					FileType: "lan-supervisord",
					FileName: "bin/supervisord",
					FileMode: "0755",
					Content:  "",
					CosKey:   "supervisord",
				})
			}
		}
		res = append(res, &Task{Task: task, TaskFiles: files, TaskPath: taskPath})
	}
	return res, err
}

func shouldUpdateNodeconfig(serviceName, taskIDC, cfName string) bool {
	if serviceName != def.SERVICE_STORED_PROXY {
		if strings.Contains(cfName, ".toml") {
			return true
		} else {
			return false
		}
	}
	if strings.Contains(cfName, taskIDC) {
		return true
	}
	return false
}

func render(source string, data interface{}) (string, error) {
	tmpl, err := template.New("").Parse(source)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, data)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func UpdatePorts(taskId, servicePort, clusterPort uint) error {
	info, err := tbl_task.GetInfo(taskId)
	if err != nil {
		log.Warn("get task info failed.err:", err, " taskId:", taskId)
		return err
	}
	info.TaskExt.ServicePort = servicePort
	info.TaskExt.ClusterPort = clusterPort
	info.Status = def.TASK_SUCCESS
	return tbl_task.UpdateTask(taskId, info)
}

func CheckGroup(currentTaskInfo *tbl_task.Task, extString string) (bool, error) {
	taskList, e := tbl_task.GetListByGroup(currentTaskInfo.ClusterId, currentTaskInfo.GroupId)
	if e != nil {
		return false, e
	}

	var list []string
	var nodeIds []uint
	for _, taskInfo := range taskList {
		if taskInfo.Status != def.TASK_SUCCESS {
			return false, nil
		}
		address := fmt.Sprintf("%s:%d", taskInfo.TaskExt.Ip, taskInfo.TaskExt.ClusterPort)
		nodeIds = append(nodeIds, taskInfo.NodeId)
		list = append(list, address)
	}
	initNodeIds := FormatNodeId(nodeIds)

	for _, taskInfo := range taskList {
		taskInfo.Type = def.TASK_TYPE_START
		taskInfo.Status = def.TASK_NEW
		taskInfo.TaskExt.NodeList = list
		taskInfo.TaskExt.ExtString = extString
		taskInfo.TaskExt.NodeIdList = initNodeIds
		taskInfo.TaskExt.NodeListStr = fmt.Sprintf("[\"%v\"]", strings.Join(taskInfo.TaskExt.NodeList, "\",\""))
		taskInfo.TaskExt.NodeListVal = fmt.Sprintf("%s:%d", taskInfo.TaskExt.Ip, taskInfo.TaskExt.ClusterPort)
		err := tbl_task.UpdateTask(taskInfo.ID, taskInfo)
		if err != nil {
			msg := fmt.Sprintf("[New server node]-[task/prepared interface]-Failed to update task table NodeList, taskId:%d", taskInfo.ID)
			_ = rpc.SendDingding(rpc.OpErrTitle, msg)
		}
	}
	err := tbl_group.Update(currentTaskInfo.ClusterId, currentTaskInfo.GroupId, tbl_group.Group{
		InitRaft:   fmt.Sprintf("[\"%v\"]", strings.Join(list, "\",\"")),
		InitNodeId: initNodeIds,
	})
	return true, err
}

func FormatNodeId(nodeId []uint) string {
	if len(nodeId) == 1 {
		return fmt.Sprintf("[%d]", nodeId[0])
	}
	strSlice := make([]string, len(nodeId))
	for i, v := range nodeId {
		strSlice[i] = strconv.Itoa(int(v))
	}
	result := strings.Join(strSlice, ",")
	s := fmt.Sprintf("[%s]", result)
	return s
}

func Started(taskId uint, status string) error {
	return tbl_task.Update(taskId, func(task *tbl_task.Task) bool {
		task.Status = status
		return true
	})
}

func UpdateTaskStatus(taskId uint, status string) error {
	return tbl_task.Update(taskId, func(task *tbl_task.Task) bool {
		task.Status = status
		return true
	})
}

func FormatCreateNodeTask(basicTask *tbl_task.Task, isWitness bool, serviceInfo *tbl_service.Service, clusterInfo *tbl_cluster.Cluster) (*tbl_task.Task, string, error) {
	task := basicTask
	task.Status = def.TASK_NEW
	var msg string
	switch basicTask.ServiceId {
	case def.SERVICE_ID_PROXY:
		task.Type = def.TASK_TYPE_START
		task.GroupId = 1
		task.TaskExt.DashboardAddress = config.GetConf().Domains.Dashboard
		isAlive := net2.CheckRemotePortInUse(task.TaskExt.Ip, int(task.TaskExt.ServicePort))
		if isAlive {
			msg = "port exists"
			return nil, msg, nil
		}
		isAlive = net2.CheckRemotePortInUse(task.TaskExt.Ip, int(task.TaskExt.ClusterPort))
		if isAlive {
			msg = "port exists"
			return nil, msg, nil
		}
		task.TaskExt.ExtString = mdl_resource_pool.FormatMachineCgroup(task.TaskExt.ServicePort, task.ClusterId, task.MachineId, task.TaskExt.CloudType)
		task.TaskExt.Operation = def.OPERATION_SUPERVISOR_START
		err := tbl_node.Update(task.NodeId, task.GroupId, task.ClusterId, tbl_node.Node{ServicePort: task.TaskExt.ServicePort, ClusterPort: task.TaskExt.ClusterPort})
		if err != nil {
			msg = "update tbl_node failed"
			return nil, msg, err
		}
	case def.SERVICE_ID_MATRIX, def.SERVICE_ID_BITALOS:
		task.Type = def.TASK_TYPE_PREPAREADD
		task.TaskExt.ServicePortRange = mdl_port.NarrowDownPortRange(serviceInfo.PortRanges, task.MachineId)
		task.TaskExt.ClusterPortRange = mdl_port.NarrowDownPortRange(serviceInfo.ClusterPortRanges, task.MachineId)
		task.TaskExt.NodeIndex = int(task.NodeId)
		task.TaskExt.ServicePort = 0
		task.TaskExt.ClusterPort = 0
		var initRaft []string
		initRaft, groupInfo, err := mdl_group.GetGroupInfo(task.ClusterId, task.GroupId, task.ServiceId)
		if err != nil {
			msg = "init raft failed"
			return nil, msg, err
		}
		task.TaskExt.NodeList = initRaft
		task.TaskExt.NodeListStr = groupInfo.InitRaft
		task.TaskExt.NodeIdList = groupInfo.InitNodeId
		task.TaskExt.IsWitness = false
		task.TaskExt.IsObserver = true
		if isWitness {
			task.TaskExt.IsWitness = true
			task.TaskExt.IsObserver = false
			err = tbl_node.Update(task.NodeId, task.GroupId, task.ClusterId, tbl_node.Node{IsWitness: true})
			if err != nil {
				msg = "update node failed"
				return nil, msg, err
			}
		}
		task.TaskExt.DeraftToken = math2.GetMd5(task.TaskExt.ClusterName)
		task.TaskExt.DashboardAddress = config.GetConf().Domains.Dashboard
		dashboardName, err := mdl_dashboard.GetDashboardName(clusterInfo.StoredId)
		if err != nil {
			log.Warn("get dashboard name failed.err:", err)
			dashboardName = task.TaskExt.ClusterName
		}
		task.TaskExt.DashboardName = dashboardName
		task.TaskExt.Operation = def.OPERATION_SUPERVISOR_START
	case def.SERVICE_ID_DASHBOARD, def.SERVICE_ID_FE:
		task.Type = def.TASK_TYPE_START
		task.TaskExt.ClusterPort = 0
		task.TaskExt.Operation = def.OPERATION_START
		err := tbl_node.Update(task.NodeId, task.GroupId, task.ClusterId, tbl_node.Node{ServicePort: task.TaskExt.ServicePort})
		if err != nil {
			msg = "dh or fe update node failed"
			return nil, msg, err
		}
		err = tbl_config.UpdateClusterId(clusterInfo.ConfigPackId, task.ClusterId, task.ServiceId)
		if err != nil {
			msg = "dh or fe update tbl_config failed"
			return nil, msg, err
		}
	}
	return task, "", nil
}
