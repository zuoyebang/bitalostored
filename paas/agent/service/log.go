package service

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/errors"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"github.com/zuoyebang/bitalostored/paas/agent/service/collector"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type QueryResp struct {
	LogText string `json:"logText"`
	LogNums int    `json:"logNums"`
}

func LogQuery(ctx *gin.Context, port, query string, queryTime int64) ([]byte, []byte, error) {
	var bitalosQuery bool
	var file, fileType string
	deployInfo, err := collector.GetNodeDeployPath()
	if err != nil {
		return nil, nil, err
	}
	if deployInfo.Status != 0 {
		return nil, nil, errors.New("get paas nodedeployinfo status != 0")
	}
	intPort, _ := strconv.ParseInt(port, 10, 64)
	queryDir := ""
	for _, proxy := range deployInfo.Data.ProxyLog {
		if proxy.Port == intPort {
			queryDir = proxy.Path
			break
		}
	}
	for _, server := range deployInfo.Data.ServerLog {
		if server.Port == intPort {
			bitalosQuery = true
			queryDir = server.Path
			break
		}
	}

	tm := time.Unix(queryTime, 0)
	logTime := tm.Format("2006010215")
	if bitalosQuery {
		fileType = "bitalos"
		if query == "tree" {
			treeCmd := fmt.Sprintf("tree -h %s", queryDir)
			res, err := execCmd(treeCmd)
			return res, nil, err
		}
		logTime = tm.Format("20060102")
		switch query {
		case "slow":
			file = queryDir + "/bitalosdb/log/bitalos.log.slow." + logTime
			query = ""
		case "error":
			file = queryDir + "/bitalosdb/log/bitalos.log.err." + logTime
			query = ""
		default:
			file = queryDir + "/bitalosdb/log/bitalos.log." + logTime
		}
	} else { //proxy
		fileType = "proxy"
		if query == "slow" {
			file = queryDir + "/proxy.slow.log." + logTime
			query = ""
		} else {
			file = queryDir + "/stored-proxy.log." + logTime
		}
	}
	return realQuery(file, fileType, query, queryTime)
}

func realQuery(file, fileType, query string, queryTime int64) ([]byte, []byte, error) {
	var err error
	var cmd, cmdNum string
	var numRes, res []byte
	_, err = os.Stat(file)
	if err != nil || os.IsNotExist(err) {
		logs.Warnf("%s File %s not exists", fileType, file)
		return nil, nil, errors.New(fileType + "file not exists " + file)
	}
	tm := time.Unix(queryTime, 0)
	secondFmt := tm.Format("05")
	minuteFmt := tm.Format("04")
	dateFormat := "2006-01-02 "
	if fileType == "proxy" {
		dateFormat = "2006/01/02 "
	}
	timeFormat := tm.Format(dateFormat + "15:04:05")
	queryHour := false
	if secondFmt == "00" {
		timeFormat = tm.Format(dateFormat + "15:04")
		if minuteFmt == "00" {
			queryHour = true
			timeFormat = tm.Format(dateFormat + "15")
		}
	}
	if len(query) <= 0 {
		cmd = fmt.Sprintf("grep '%s' %s |grep -v bash", timeFormat, file)
		cmdNum = fmt.Sprintf("grep '%s' %s |grep -v bash|wc -l", timeFormat, file)
		if queryHour && fileType == "proxy" {
			cmd = fmt.Sprintf("cat %s", file)
			cmdNum = fmt.Sprintf("cat %s|wc -l", file)
		}
	} else {
		cmd = fmt.Sprintf("grep -i %s %s | grep '%s'|grep -v bash", query, file, timeFormat)
		cmdNum = fmt.Sprintf("grep -i %s %s | grep '%s'|grep -v bash|wc -l", query, file, timeFormat)
		if queryHour && fileType == "proxy" {
			cmd = fmt.Sprintf("grep -i %s %s |grep -v bash", query, file)
			cmdNum = fmt.Sprintf("grep -i %s %s|grep -v bash|wc -l", query, file)
		}
	}
	numRes, _ = execCmd(cmdNum)
	strNum := string(numRes)
	strNum = strings.Replace(strNum, "\n", "", -1)
	intNum, e := strconv.ParseInt(strNum, 10, 64)
	logs.Infof("logquery %s cmd:%s num:%d strnum:%s err:%v", fileType, cmdNum, intNum, strNum, e)
	if intNum > 1000 {
		logs.Infof("logquery %s cmd:%s", fileType, cmd)
		res, err = execCmd(cmd + "|head -1000")
	} else {
		logs.Infof("logquery %s cmd:%s", fileType, cmd)
		res, err = execCmd(cmd)
	}
	return res, numRes, err
}

func matchBitalosDir(port string) string {
	cmd := fmt.Sprintf("ps -ef |grep %s |grep 'bin/stored-bitalos'|awk '{print $8}'|grep -v bash", port)
	res, err := exec.Command("bash", "-c", cmd).Output()
	logs.Infof("logquery matchBitalosDir:%s", cmd)
	if err != nil {
		if err.Error() == "exit status 1" {
			logs.Warn("logquery matchBitalosDir empty")
			return ""
		}
		return err.Error()
	}
	if len(res) <= 0 {
		logs.Warn("logquery matchBitalosDir empty")
		return ""
	}
	dir := string(res)
	logs.Infof("logquery matchBitalosDir:%s", dir)
	bitalosDir := dir[0 : len(dir)-len("/bin/stored-bitalos")]
	return bitalosDir
}

func execCmd(cmd string) ([]byte, error) {
	res, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		if err.Error() == "exit status 1" {
			return nil, nil
		}
		logs.Warnf("logquery fail cmd:%s, err:%v", cmd, err.Error())
		return nil, err
	}
	if len(res) <= 0 {
		logs.Warnf("logquery empty, cmd:%s", cmd)
		return nil, nil
	}
	return res, nil
}
