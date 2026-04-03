package task

import (
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/config"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/def"
	"path"
)

func (task *TaskInfo) getProgramPath() string {
	if task.TaskExt.ServiceName == def.SERVICE_MATRIX || task.TaskExt.ServiceName == def.SERVICE_BITALOS {
		return path.Join(config.C.DeployPath, task.TaskExt.RegionName, task.TaskExt.ServiceName, task.TaskExt.ClusterName,
			fmt.Sprintf("group-%d/node-%d-port-%d", task.GroupId, task.NodeId, task.TaskExt.ServicePort))
	}
	return path.Join(config.C.DeployPath, task.TaskExt.ServiceName, task.TaskExt.ClusterName,
		fmt.Sprintf("/node-port-%d", task.TaskExt.ServicePort))
}

func (task *TaskInfo) getTargetBitalosPath() string {
	if task.TaskExt.ServiceName != def.SERVICE_BITALOS || task.TaskExt.TargetGroupId == 0 {
		return ""
	}
	return path.Join(config.C.DeployPath, task.TaskExt.RegionName, task.TaskExt.ServiceName, task.TaskExt.ClusterName,
		fmt.Sprintf("group-%d/node-%d-port-%d", task.TaskExt.TargetGroupId, task.NodeId, task.TaskExt.ServicePort))
}
