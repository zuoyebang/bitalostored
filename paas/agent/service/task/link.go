package task

import (
	"github.com/zuoyebang/bitalostored/paas/agent/internal/config"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/def"
	log "github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/webclient"
	"os"
	"os/exec"
	"path"
)

func ApplyLink(task *TaskInfo) {
	var status string
	err := execLink(task)
	if err != nil {
		status = "fail"
	} else {
		status = "success"
	}
	webclient.PostPaaS(config.GetPaaSAddress(def.URL_STATUS), map[string]interface{}{
		"taskId": task.TaskId, "taskIds": []int64{task.TaskId}, "taskStatus": status, "errors": "",
	})
}

func execLink(task *TaskInfo) error {
	srcDirName := task.getProgramPath()
	targetDirName := task.getTargetBitalosPath()
	if err := os.MkdirAll(path.Dir(targetDirName), 0755); err != nil {
		return err
	}
	cmd := exec.Command("ln", "-s", srcDirName, targetDirName)
	log.Infof("link -s %s %s", srcDirName, targetDirName)
	err := cmd.Run()
	if err != nil {
		log.Warnf("link err:%s src:%s target:%s", err, srcDirName, targetDirName)
	}
	return err
}
