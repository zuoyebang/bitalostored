package utils

import (
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"io/ioutil"
	"log"
	"os/exec"
)

func PageLimit(offset, limit int) (int, int) {
	if offset < 0 {
		offset = 0
	}
	if limit > 100 {
		limit = 100
	}
	if limit <= 0 {
		limit = 10
	}
	return offset, limit
}
func Argument(d map[string]interface{}, name string) (string, bool) {
	if d[name] != nil {
		if s, ok := d[name].(string); ok {
			if s != "" {
				return s, true
			}
			log.Panicf("option %s requires an argument", name)
		} else {
			log.Panicf("option %s isn't a valid string", name)
		}
	}
	return "", false
}

func RunCmd(cmd string) []byte {
	execommand := exec.Command("/bin/bash", "-c", cmd)
	stdout, err := execommand.StdoutPipe()
	if err != nil {
		logs.Warnf("Error:can not obtain stdout pipe for command:%s\n", err)
		return nil
	}

	if err := execommand.Run(); err != nil {
		logs.Warnf("Error:The command is err, %s", err.Error())
		return nil
	}

	bytes, err := ioutil.ReadAll(stdout)
	if err != nil {
		logs.Warnf("ReadAll Stdout:%s", err.Error())
		return nil
	}

	if err := execommand.Wait(); err != nil {
		logs.Warnf("wait:%s", err.Error())
		return nil
	}
	return bytes
}
