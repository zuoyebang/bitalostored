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

package mdl_ops

import (
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_node"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_opsactionlog"
	"github.com/zuoyebang/bitalostored/paas/dao/tbl_task"
	"github.com/zuoyebang/bitalostored/paas/utils/def"
	"time"
)

func CreateOpsActionLog(node *tbl_node.Node, task *tbl_task.Task) {
	actionType := make([]int, 0)
	switch task.ServiceId {
	case def.SERVICE_ID_PROXY:
		if task.TaskExt.UpdateConfig {
			actionType = append(actionType, def.ProxyConfUpdate)
		}
		if task.Type == def.TASK_TYPE_START {
			actionType = append(actionType, def.ProxyAddNode)
		} else if task.Type == def.TASK_TYPE_UPGRADE || task.Type == def.TASK_TYPE_OPERATE {
			if task.TaskExt.Operation == def.OPERATION_STOP {
				if task.CosFileId != node.CosFileId {
					actionType = append(actionType, def.ProxyNodeUpgrade)
				} else {
					actionType = append(actionType, def.ProxyRestart)
				}
			}
		}
		break
	case def.SERVICE_ID_BITALOS, def.SERVICE_ID_MATRIX:
		if task.TaskExt.UpdateConfig {
			actionType = append(actionType, def.ServerConfUpdate)
		}
		if task.Type == def.TASK_TYPE_UPGRADE {
			if task.CosFileId != node.CosFileId {
				actionType = append(actionType, def.ServerNodeUpgrade)
			} else {
				actionType = append(actionType, def.ServerRestart)
			}
		} else if task.Type == def.TASK_TYPE_PREPAREADD {
			actionType = append(actionType, def.ServerAddNode)
		} else if task.Type == def.TASK_TYPE_PREPARESTART {
			actionType = append(actionType, def.ServerAddSharding)
		}
		break
	}
	opTime := time.Now().Unix()
	if len(actionType) <= 0 {
		return
	}
	for _, actType := range actionType {
		actionLog := &tbl_opsactionlog.OpsActionLog{
			Ip:          task.TaskExt.Ip,
			Port:        task.TaskExt.ServicePort,
			ClusterName: task.TaskExt.ClusterName,
			ActionType:  actType,
			OpName:      "",
			UpdateTime:  opTime,
			CreateTime:  opTime,
		}
		tbl_opsactionlog.Create(actionLog)
	}

	return
}
