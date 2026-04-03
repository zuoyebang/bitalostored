package agent

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	tbl_machine "github.com/zuoyebang/bitalostored/paas/agent/dao/machine"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/config"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/def"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/cfunc"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/connector"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/flock"
	log "github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"github.com/zuoyebang/bitalostored/paas/agent/service/task"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"time"
)

func Start(lock *flock.Flock) {
	connector.InitMysql()
	//cgroup
	machineInfo := task.GetMachineInfo()
	task.InitRootCpuSet(machineInfo)
	task.SetCgroup(machineInfo)
	task.ScanCpuStat(machineInfo.ID)

	go func(machineInfo *tbl_machine.Machine) {
		for {
			_ = task.UpdateMachineTime(machineInfo)
			time.Sleep(time.Second * 10)
		}
	}(machineInfo)

	agentMD5 := calMD5()
	for {
		_, err := task.GetList()
		log.Infof("agent md5:%s version:%s get taskList done err:%v", agentMD5, utils.Version, err)
		time.Sleep(time.Second * 10)
	}
}

var RecoveryFlag bool

func SetRecovery() {
	RecoveryFlag = true
}

func RecoveryNode() {
	if !RecoveryFlag {
		return
	}
	log.Info("start recovery machine node")
	p := fmt.Sprintf("%s?machineId=%d", config.GetPaaSAddress(def.URL_RECOVERY), config.C.MachineId)
	log.Infof("recoveryurl = %s", p)
	_, err := http.Get(p)
	if err != nil {
		log.Warnf("request recovery url failed, err=%v", err)
	}
}

func Upgrade() bool {
	cmd := exec.Command(config.C.AgentPath + "/restart.sh")
	err := cmd.Run()
	time.Sleep(time.Second * 3)
	if err != nil {
		return false
	}
	return true
}

func calMD5() string {
	file, err := os.Open(config.C.AgentPath + "/bin/bitalosagent")
	if err != nil {
		log.Warn("open file error:%+v", err)
		return ""
	}
	md5hash := md5.New()
	_, err = io.Copy(md5hash, file)
	if err != nil {
		log.Warn("copy file error:%+v", err)
		return ""
	}
	md5 := hex.EncodeToString(md5hash.Sum(nil))
	file.Close()
	return md5
}

func upgradeAgent() bool {
	p := fmt.Sprintf("%s?machineId=%d&version=%s",
		config.GetPaaSAddress(def.URL_MANAGE), config.C.MachineId, utils.Version)
	res, err := http.Get(p)
	if err != nil {
		log.Warn("agent upgrade", err)
		return false
	}

	manageInfo := &ManageAgentResp{}
	if err = json.NewDecoder(res.Body).Decode(manageInfo); err != nil {
		log.Warn("agent upgrade", err, p)
		return false
	}
	//log.Info("manage info:", manageInfo)
	if manageInfo.Data.Content == "" && manageInfo.Data.CosKey == "" {
		return false
	}
	downloadAgentConfig(config.C.AgentPath+"/conf/config.toml", manageInfo.Data.Content, 0644)
	downloadAgent(config.C.AgentPath+"/bin/stored-agent", manageInfo.Data.CosKey, 0775)
	paas := fmt.Sprintf("%s?machineId=%d", config.GetPaaSAddress(def.URL_UPDATED), config.C.MachineId)
	http.Get(paas)
	return true
}

func downloadAgentConfig(downloadPath, content string, mode os.FileMode) bool {
	if content == "" {
		log.Warn("empty config")
		return true
	}
	var err error
	if cfunc.IsExist(downloadPath) {
		// If the file already exists, remove it first.
		err = os.Remove(downloadPath)
		if err != nil {
			log.Warn("remove", err)
			return false
		}
	}
	err = os.MkdirAll(path.Dir(downloadPath), 0755)
	if err != nil {
		log.Warn("mkdir", err)
		return false
	}
	f, e := os.Create(downloadPath)
	if e != nil {
		log.Warn("create failed.", e)
		return false
	}
	defer f.Close()
	f.WriteString(content)
	if e := os.Chmod(downloadPath, mode); e != nil {
		log.Warn("chmod", e)
		return false
	}
	return true
}

func downloadAgent(downloadPath, cosKey string, mode os.FileMode) bool {
	if cosKey == "" {
		return true
	}
	var err error
	if cfunc.IsExist(downloadPath) {
		// If the file already exists, remove it first.
		err = os.Remove(downloadPath)
		if err != nil {
			log.Warn("remove", err)
			return false
		}
	}
	err = os.MkdirAll(path.Dir(downloadPath), 0755)
	if err != nil {
		log.Warn("mkdir", err)
		return false
	}
	err = task.DownloadFile(cosKey, downloadPath)
	if err != nil {
		log.Warn("download cos file.cosKey:", cosKey, " err:", err)
		return false
	}
	if e := os.Chmod(downloadPath, mode); e != nil {
		log.Warn("chmod", e)
		return false
	}
	return true
}

type ManageAgentResp struct {
	Status int          `json:"status"`
	Msg    string       `json:"msg"`
	Data   ManageParams `json:"data"`
}

type ManageParams struct {
	CosKey  string `json:"cosKey"`
	Content string `json:"content"` // agent config file
}
