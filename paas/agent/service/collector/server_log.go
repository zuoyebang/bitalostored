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

type SQueue struct {
	QueueChan chan *QData
	QuitC     chan struct{}
}

func NewSQueue() *SQueue {
	queue := &SQueue{
		QuitC:     make(chan struct{}, 1),
		QueueChan: make(chan *QData, ChanLen),
	}
	logs.Info("start consume server slow && error log")
	StartConsume(queue)
	return queue
}

func (q *SQueue) SourceType() string {
	return "server"
}

func (q *SQueue) GetQuitC() chan struct{} {
	return q.QuitC
}

func (q *SQueue) GetQchan() chan *QData {
	return q.QueueChan
}

func (q *SQueue) Push(data *QData) {
	if q == nil {
		return
	}
	q.QueueChan <- data
}

func (q *SQueue) InsertLog(qdatas []*QData) {
	//logs.Infof("insert server log qdatas length:%d", len(qdatas))
	var slowDatas, expDatas []*QData
	for _, data := range qdatas {
		if data.logType == def.ServerSlowLogType {
			slowDatas = append(slowDatas, data)
		}
		if data.logType == def.ServerErrLogType {
			data.logType = def.ServerLogType
			expDatas = append(expDatas, data)
		}
	}
	if len(slowDatas) > 0 {
		insertServerSlowLog(slowDatas)
	}
	if len(expDatas) > 0 {
		insertServerExpLog(expDatas)
	}
}

func insertServerSlowLog(qdatas []*QData) {
	var err error
	service := collect.NewSlowLogService()
	now := time.Now().Unix()
	for _, data := range qdatas {
		r := collect.SlowLogModel{
			ClusterName: data.clusterName,
			Service:     def.LOG_SERVICE_SERVER,
			NodeIp:      LocalIpCollector,
			NodePort:    data.port,
			IDC:         IdcCollector,
			Duration:    data.duration,
			Key:         data.key,
			Command:     data.command,
			Params:      data.params,
			LogTime:     data.logTime,
			CreateTime:  now,
		}
		err = service.Insert(r)
		if err != nil {
			logs.Errorf("insert server slow log, record:%+v err:%v", r, err)
		}
	}
}

func insertServerExpLog(qdatas []*QData) {
	service := collect.NewExceptionLogService()
	var err error
	now := time.Now().Unix()
	for _, data := range qdatas {
		record := collect.ExceptionLogModel{
			ClusterName:   data.clusterName,
			LogType:       data.logType,
			LogLevel:      data.logLevel,
			NodeIp:        LocalIpCollector,
			Key:           data.key,
			Command:       data.command,
			DstIp:         data.dstIp,
			DstPort:       data.dstPort,
			IDC:           IdcCollector,
			NodePort:      data.port,
			ExceptionInfo: data.exceptionInfo,
			RawContent:    data.rawContent,
			LogTime:       data.logTime,
			CreateTime:    now,
		}
		err = service.Insert(record)
		if err != nil {
			logs.Infof("insert exception log record:%+v, err:%v", record, err)
		}
	}
}

func (q *SQueue) Close() {
	close(q.QuitC)
}

func (q *SQueue) HandleServerSlowLog(logLine, clusterName string, port int64) {
	data := parseServerSlowLog(logLine)
	if data == nil {
		return
	}
	data.clusterName = clusterName
	data.port = port
	data.logType = def.ServerSlowLogType
	q.Push(data)
}

func (q *SQueue) HandleServerErrLog(logLine, clusterName string, port int64) {
	data := parseServerErrLog(logLine)
	if data == nil {
		return
	}
	data.clusterName = clusterName
	data.port = port
	data.logType = def.ServerErrLogType
	q.Push(data)
}

func (q *SQueue) HandleServerSupervisorLog(logLine, clusterName string, port int64) {
	data := parseSupervisorLog(logLine)
	if data == nil {
		return
	}
	data.clusterName = clusterName
	data.port = port
	data.logType = def.ServerErrLogType
	q.Push(data)
}

func parseSupervisorLog(logText string) *QData {
	if len(logText) <= 0 {
		return nil
	}
	errIndex := strings.Index(logText, "panic")
	fatalIndex := strings.Index(logText, "fatal")
	if errIndex == -1 || fatalIndex == -1 {
		return nil
	}
	return &QData{
		logLevel:   def.LogLevelPanic,
		rawContent: logText,
	}
}

/*
2023-05-08 14:42:49.777259 E | 52744 bitalos-server/server/proc.go:68 ^[[31mclient run panic runtime error: invalid memory address or nil pointer dereference:goroutine 893 [running]:
*/
func parseServerErrLog(logText string) *QData {
	if len(logText) <= 0 {
		return nil
	}
	parts := strings.Split(logText, " ")
	if len(parts) < 2 {
		logs.Warnf("log parse failed, text=%s", logText)
		return nil
	}
	dateStr := parts[0]
	timeStr := parts[1]
	dateTimeStr := dateStr + " " + timeStr
	loc, _ := time.LoadLocation("Asia/Shanghai")
	dateTime, err := time.ParseInLocation("2006-01-02 15:04:05.000000", dateTimeStr, loc)
	if err != nil {
		logs.Errorf("parse time  failed, err:%v", err)
		return nil
	}
	unixTimestamp := dateTime.Unix()

	logLevel := def.LogLevelError
	var exceptionInfo, dstIp string
	var dstPort int64
	for {
		//panic
		errIndex := strings.Index(logText, " panic ")
		if errIndex != -1 {
			logLevel = def.LogLevelPanic
			break
		}
		errIndex = strings.Index(logText, "runtime error")
		if errIndex != -1 {
			logLevel = def.LogLevelPanic
			break
		}
		errorRegex := regexp.MustCompile(`handleRequest error:(.*)`)
		match := errorRegex.FindStringSubmatch(logText)
		if len(match) > 0 {
			re := regexp.MustCompile(`\^\[\[\d+m`)
			exceptionInfo = match[1]
			exceptionInfo = re.ReplaceAllString(exceptionInfo, "")
			break
		}
		re := regexp.MustCompile(`failed to get a connection to ([\d.:]+), (.+)`)
		match = re.FindStringSubmatch(logText)
		if match != nil {
			ipPorts := match[1]
			sp := strings.Split(ipPorts, ":")
			dstIp = sp[0]
			dstPort, _ = strconv.ParseInt(sp[1], 10, 64)
			exceptionInfo = def.ExceptionRaftConnect
		}
		break
	}
	return &QData{
		logLevel:      logLevel,
		logTime:       unixTimestamp,
		dstIp:         dstIp,
		dstPort:       dstPort,
		exceptionInfo: exceptionInfo,
		rawContent:    logText,
	}
}

/*
2023-05-08 16:52:44.172342 S | 52744 bitalos-server/server/client.go:259 [ip_port:127.0.0.1:51686] [duration(us):700194] [raftsync(us):0] [query:"freememory"] [status:OK]
*/
func parseServerSlowLog(logText string) *QData {
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
		logs.Errorf("parse time  failed, err:%v", err)
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
