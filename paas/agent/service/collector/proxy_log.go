package collector

import (
	"github.com/zuoyebang/bitalostored/paas/agent/dao/collect"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/def"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type PsQueue struct {
	QueueChan chan *QData
	QuitC     chan struct{}
}

func NewPsQueue() *PsQueue {
	queue := &PsQueue{
		QuitC:     make(chan struct{}, 1),
		QueueChan: make(chan *QData, ChanLen),
	}
	logs.Info("start consume proxy slow log")
	StartConsume(queue)
	return queue
}

func (q *PsQueue) SourceType() string {
	return "proxy"
}

func (ps *PsQueue) GetQuitC() chan struct{} {
	return ps.QuitC
}

func (ps *PsQueue) GetQchan() chan *QData {
	return ps.QueueChan
}

func (ps *PsQueue) Push(data *QData) {
	if ps == nil {
		return
	}
	ps.QueueChan <- data
}

func (ps *PsQueue) InsertLog(qdatas []*QData) {
	// logs.Infof("insert proxy log qdatas length:%d", len(qdatas))
	var slowDatas, expDatas []*QData
	for _, data := range qdatas {
		if data.logType == def.ProxySlowLogType {
			slowDatas = append(slowDatas, data)
		}
		if data.logType == def.ProxyErrLogType {
			data.logType = def.ProxyLogType
			expDatas = append(expDatas, data)
		}
	}
	if len(slowDatas) > 0 {
		insertProxySlowLog(slowDatas)
	}
	if len(expDatas) > 0 {
		insertProxyExpLog(expDatas)
	}
}

func insertProxySlowLog(qdatas []*QData) {
	service := collect.NewSlowLogService()
	now := time.Now().Unix()
	for _, data := range qdatas {
		record := collect.SlowLogModel{
			ClusterName: data.clusterName,
			Service:     def.LOG_SERVICE_PROXY,
			NodeIp:      LocalIpCollector,
			IDC:         IdcCollector,
			NodePort:    data.port,
			Duration:    data.duration,
			Key:         data.key,
			Command:     data.command,
			Params:      data.params,
			LogTime:     data.logTime,
			CreateTime:  now,
			UpdateTime:  0,
		}
		// logs.Infof("insert proxy log:%+v", record)
		service.Insert(record)
	}
}

func insertProxyExpLog(qdatas []*QData) {
	service := collect.NewExceptionLogService()
	var err error
	now := time.Now().Unix()
	for _, data := range qdatas {
		nodeIp := LocalIpCollector
		idc := IdcCollector
		/*
			if data.exceptionInfo == def.ExceptionMasterChange {
				nodeIp = data.ip
				idc = transformIdc(data.ip)
			}

		*/
		record := collect.ExceptionLogModel{
			ClusterName:   data.clusterName,
			LogType:       data.logType,
			LogLevel:      data.logLevel,
			NodeIp:        nodeIp,
			GroupId:       data.gid,
			SlotId:        data.slotId,
			Key:           data.key,
			DstIp:         data.dstIp,
			IDC:           idc,
			NodePort:      data.port,
			Command:       data.command,
			DstPort:       data.dstPort,
			ExceptionInfo: data.exceptionInfo,
			RawContent:    data.rawContent,
			LogTime:       data.logTime,
			CreateTime:    now,
			UpdateTime:    0,
		}
		err = service.Insert(record)
		if err != nil {
			logs.Infof("insert exception log data:%+v, err:%v", record, err)
		}
	}
}

func (ps *PsQueue) Close() {
	close(ps.QuitC)
}

func (ps *PsQueue) HandleProxySlowLog(logText, clusterName string, port int64) {
	// logs.Infof("proxy log:%s cluster:%s", logText, clusterName)
	data := parseProxySlowLog(logText)
	if data == nil {
		return
	}
	data.clusterName = clusterName
	data.port = port
	data.logType = def.ProxySlowLogType
	ps.Push(data)
}

func (ps *PsQueue) HandleProxyErrLog(logText, clusterName string, port int64) {
	// logs.Infof("proxy log:%s cluster:%s", logText, clusterName)
	data := parseProxyErrLog(logText)
	if data == nil {
		return
	}
	data.clusterName = clusterName
	if data.exceptionInfo != def.ExceptionMasterChange {
		data.port = port
	}
	data.logType = def.ProxyErrLogType
	ps.Push(data)
}

func parseProxySlowLog(logText string) *QData {
	if len(logText) <= 0 {
		return nil
	}
	parts := strings.Split(logText, " ")
	if len(parts) < 3 {
		logs.Warnf("log parse failed, text=%s", logText)
		return nil
	}
	dateStr := parts[0]
	timeStr := parts[1]
	dateTimeStr := dateStr + " " + timeStr
	loc, _ := time.LoadLocation("Asia/Shanghai")
	dateTime, err := time.ParseInLocation("2006-01-02 15:04:05.000000", dateTimeStr, loc)
	if err != nil {
		logs.Errorf("parse new time  failed, err:%v", err)
		return nil
	}
	unixTimestamp := dateTime.Unix()

	var d int64
	var key, command, params string
	durationRegex := regexp.MustCompile(`duration\(us\):(\d+).*query:"(.*)"`)
	match := durationRegex.FindStringSubmatch(logText)
	if len(match) == 3 {
		durationStr := match[1]
		d, _ = strconv.ParseInt(durationStr, 10, 64)
		queryStr := match[2]
		queryParts := strings.Split(queryStr, " ")
		command = queryParts[0]
		if len(queryParts) > 1 {
			key = queryParts[1]
		}
		if len(queryParts) > 2 {
			params = strings.Join(queryParts[2:], " ")
		}
	}

	return &QData{
		logTime:  unixTimestamp,
		duration: d,
		command:  command,
		key:      key,
		params:   params,
	}
}

func parseProxyErrLog(logText string) *QData {
	if len(logText) <= 0 {
		return nil
	}
	parts := strings.Split(logText, " ")
	if len(parts) < 4 {
		logs.Warnf("log parse failed, text=%s", logText)
		return nil
	}
	logLevel := parts[3]
	if logLevel != "[ERROR]" && logLevel != "[WARN]" && logLevel != "[PANIC]" {
		return nil
	}
	filterIndex := strings.Index(logText, "API call")
	if filterIndex != -1 {
		return nil
	}
	filterIndex = strings.Index(logText, "session.go")
	if filterIndex != -1 {
		return nil
	}
	filterIndex = strings.Index(logText, "compare.go")
	if filterIndex != -1 {
		return nil
	}
	filterIndex = strings.Index(logText, "not most agree master")
	if filterIndex != -1 {
		return nil
	}

	dateStr := parts[0]
	timeStr := parts[1]
	dateTimeStr := dateStr + " " + timeStr
	loc, _ := time.LoadLocation("Asia/Shanghai")
	dateTime, err := time.ParseInLocation("2006-01-02 15:04:05.000000", dateTimeStr, loc)
	if err != nil {
		logs.Errorf("parse new time  failed, err:%v", err)
		return nil
	}
	unixTimestamp := dateTime.Unix()

	var exceptionInfo, dstIp, srcIp, commandName, key string
	var dstPort, srcPort, gid, slotId int64
	for {
		errIndex := strings.Index(logText, "panic")
		if errIndex != -1 {
			logLevel = "[PANIC]"
			break
		}
		failLog := strings.Index(logText, "do redis cmd fail")
		if failLog != -1 {
			re := regexp.MustCompile(`do redis cmd fail addr:([^,]+) slotId:(\d+) commandName:([^,]+) args:\[(.*)\] err:(.*)`)
			matches := re.FindStringSubmatch(logText)
			if len(matches) > 3 {
				addr := matches[1]
				commandName = matches[3]
				args := strings.Split(matches[4], " ")
				exceptionInfo = matches[5]
				if len(exceptionInfo) <= 0 || exceptionInfo == "EOF" {
					return nil
				}
				sp := strings.Split(addr, ":")
				if len(sp) > 1 {
					dstIp = sp[0]
					dstPort, _ = strconv.ParseInt(sp[1], 10, 64)
				}
				slotId, _ = strconv.ParseInt(matches[2], 10, 64)
				key = args[0]
				errIndex := strings.Index(exceptionInfo, "i/o timeout")
				if errIndex != -1 {
					exceptionInfo = def.ExceptionTimeout
					break
				}
				errIndex = strings.Index(exceptionInfo, "connection refused")
				if errIndex != -1 {
					exceptionInfo = def.ExceptionRefuseConnect
					break
				}
			}
			break
		}
		//info i/o timeout
		infoRe := regexp.MustCompile(`get info fail.*addr\s*:\s*([\d.:]+)`)
		infoMatch := infoRe.FindStringSubmatch(logText)
		if len(infoMatch) > 1 {
			ipPort := infoMatch[1]
			sp := strings.Split(ipPort, ":")
			if len(sp) > 1 {
				dstIp = sp[0]
				dstPort, _ = strconv.ParseInt(sp[1], 10, 64)
			}
			key = "info"
			exceptionInfo = def.ExceptionTimeout
			errIndex := strings.Index(exceptionInfo, "connection refused")
			if errIndex != -1 {
				exceptionInfo = def.ExceptionRefuseConnect
				break
			}
			break
		}

		re := regexp.MustCompile(`groupId:(\d+) slotId:(\d+) master change (\d+\.\d+\.\d+\.\d+:\d+) to (\d+\.\d+\.\d+\.\d+:\d+)`)
		match := re.FindStringSubmatch(logText)
		if match != nil {
			group := match[1]
			gid, _ = strconv.ParseInt(group, 10, 64)
			slot := match[2]
			slotId, _ = strconv.ParseInt(slot, 10, 64)
			ipPort1 := match[3]
			srcSp := strings.Split(ipPort1, ":")
			srcIp = srcSp[0]
			srcPort, _ = strconv.ParseInt(srcSp[1], 10, 64)
			ipPort2 := match[4]
			dstSp := strings.Split(ipPort2, ":")
			dstIp = dstSp[0]
			dstPort, _ = strconv.ParseInt(dstSp[1], 10, 64)
			exceptionInfo = def.ExceptionMasterChange
			break
		}
		break
	}
	if len(exceptionInfo) <= 0 && logLevel != "[PANIC]" {
		return nil
	}

	if len(logText) > 1024 {
		logText = logText[0:1024]
	}

	return &QData{
		logLevel:      def.TransferProxyLogLevel(logLevel),
		logTime:       unixTimestamp,
		dstIp:         dstIp,
		dstPort:       dstPort,
		ip:            srcIp,
		gid:           gid,
		slotId:        slotId,
		port:          srcPort,
		key:           key,
		command:       commandName,
		rawContent:    logText,
		exceptionInfo: exceptionInfo,
	}
}
