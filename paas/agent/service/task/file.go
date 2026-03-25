package task

import (
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/config"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/cfunc"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
)

func (task *TaskInfo) setPath() {
	task.TaskPath = task.getProgramPath()
}

func (task *TaskInfo) UpdateFiles() error {
	task.setPath()
	for _, file := range task.TaskFiles {
		if strings.Contains(file.FileType, "template") {
			task.TemplateFiles = append(task.TemplateFiles, file.FileName)
		}
		if err := file.Update(task.TaskPath, task.TaskExt.CloudType); err != nil {
			return err
		}
	}
	return nil
}

func (file *TaskFile) Update(dir, cloudType string) error {
	var err error
	p := path.Join(dir, file.FileName)
	if cfunc.IsExist(p) {
		// If the file already exists, remove it first.
		err = os.Remove(p)
		if err != nil {
			return err
		}
	}
	err = os.MkdirAll(path.Dir(p), 0755)
	if err != nil {
		return err
	}

	// Distinguish files downloaded from COS vs local content.
	switch file.FileType {
	case "main", "compress", "supervisord":
		err := DownloadFile(file.CosKey, p)
		if err != nil {
			logs.Warn("download cos file failed.key:", file.CosKey, " err:", err)
			return err
		}
	case "lan", "lan-compress", "lan-supervisord":
		err := DownloadFromPaas(file.CosKey, p)
		if err != nil {
			logs.Warn("download file from paas failed.key:", file.CosKey, " err:", err)
			return err
		}
	default:
		f, e := os.Create(p)
		if e != nil {
			logs.Warn("create file failed.", e)
			return e
		}
		defer f.Close()
		f.WriteString(file.Content)
	}

	mode, _ := strconv.ParseUint(file.FileMode, 8, 64)
	if e := os.Chmod(p, os.FileMode(mode)); e != nil {
		logs.Warn("chmod", e)
		return e
	}

	logs.Info("fileMode:", file.FileMode, "fileName:", p, "cosKey:", file.CosKey)
	if strings.HasSuffix(file.FileName, ".tar.gz") {
		cmd := exec.Command("tar", "xf", file.FileName)
		cmd.Dir = dir
		if err := cmd.Run(); err != nil {
			logs.Warn("tar", err)
			return err
		}
	}
	if strings.HasSuffix(file.FileName, ".tar.zz") {
		targetDir := dir + "/bin"
		if !cfunc.IsExist(targetDir) {
			err = os.MkdirAll(path.Dir(targetDir), 0755)
			if err != nil {
				return err
			}
		}
		// log.Info("targetDir:",targetDir)
		cmd := exec.Command("tar", "-xf", file.FileName, "-C", targetDir)
		cmd.Dir = dir
		if err := cmd.Run(); err != nil {
			logs.Warn("tar error:", err)
			return err
		}
	}
	return nil
}

func DownloadFromPaas(name, localPath string) error {
	url := fmt.Sprintf("%sfile/download?filePath=%s", config.C.ServerAddress, name)
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("download request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("interface returned unexpected status code: %d", resp.StatusCode)
	}

	fd, err := os.OpenFile(localPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}

	_, err = io.Copy(fd, resp.Body)
	fd.Close()
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}
