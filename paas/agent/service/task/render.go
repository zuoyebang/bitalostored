package task

import (
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/def"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"strconv"
	"strings"
)

func (task *TaskInfo) PrepareFiles() error {
	err := task.UpdateFiles()
	if err != nil {
		logs.Warn("update file error:", err)
		return err
	}
	task.TaskRoot = "/home/homework/bitalos-paas/bitalos-agent"
	if task.TaskExt.ServiceName == def.SERVICE_MATRIX || task.TaskExt.ServiceName == def.SERVICE_BITALOS {
		task.TaskExt.NodeListStr = fmt.Sprintf("[\"%v\"]", strings.Join(task.TaskExt.NodeList, "\",\""))
		task.TaskExt.NodeListVal = task.TaskExt.Ip + ":" + strconv.Itoa(task.TaskExt.ClusterPort)
	}
	return nil
}
