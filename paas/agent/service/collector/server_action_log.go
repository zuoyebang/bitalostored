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

type SaQueue struct {
	QueueChan chan *QData
	QuitC     chan struct{}
}

func NewSaQueue() *SaQueue {
	queue := &SaQueue{
		QuitC:     make(chan struct{}, 1),
		QueueChan: make(chan *QData, ChanLen),
	}
	logs.Info("start consume server action log")
	StartConsume(queue)
	/*
		StartConsumeServerAction(queue)
		flushServerStartLog()

	*/
	return queue
}

func (q *SaQueue) SourceType() string {
	return "server_action"
}

func (q *SaQueue) GetQuitC() chan struct{} {
	return q.QuitC
}

func (q *SaQueue) GetQchan() chan *QData {
	return q.QueueChan
}

func (q *SaQueue) Push(data *QData) {
	if q == nil {
		return
	}
	q.QueueChan <- data
}

func (q *SaQueue) InsertLog(qdatas []*QData) {
	service := collect.NewServerActionLogService()
	var err error
	now := time.Now().Unix()
	for _, data := range qdatas {
		record := &collect.ServerActionLogModel{
			Ip:              data.ip,
			Port:            data.port,
			ClusterName:     data.clusterName,
			ActionType:      data.actionType,
			DbType:          data.dbType,
			ActionSize:      data.actionSize,
			KeyNums:         data.keyNums,
			ActionStartTime: data.actionStartTime,
			ActionEndTime:   data.actionEndTime,
			Duration:        data.actionDuration,
			Job:             data.job,
			RawContent:      data.rawContent,
			CreateTime:      now,
		}
		err = service.InsertLog(record)
		if err != nil {
			logs.Infof("insert server action log record:%+v, err:%v", record, err)
		}
	}
}

func (q *SaQueue) Close() {
	close(q.QuitC)
}

func (q *SaQueue) HandleServerActionLog(logLine, clusterName string, port int64) {
	data := parseServerActionLog(logLine)
	if data == nil {
		return
	}
	data.clusterName = clusterName
	data.port = port
	data.ip = LocalIpCollector
	data.logType = def.ServerActionLogType
	q.Push(data)
}

func parseServerActionLog(logText string) *QData {
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
	unixTimestamp := dateTime.UnixMilli()

	logLevel := def.LogLevelInfo
	var dbType, job, actionSize string
	var keyNums, actionStartTime, actionEndTime int64
	var actionDuration float64
	var actionType, actionStatus uint8
	var match []string
	for {
		//bitalosdb memtable_flush end
		pattern := `\[bitalosdb/(\w+)\].*\[(\w+\s+\d+)\].*flushed \d+ memtable to bitree.*written\(([\d.]+ \w)\).*keys\((\d+)\).*in (\d+\.\d+)s`
		re := regexp.MustCompile(pattern)
		match = re.FindStringSubmatch(logText)
		if len(match) == 6 {
			dbType = match[1]
			job = match[2]
			actionSize = match[3]
			keyNums, _ = strconv.ParseInt(match[4], 10, 64)
			actionDuration, _ = strconv.ParseFloat(match[5], 64)
			actionType = def.ActionTypeBdbMemtableFlushed
			actionStatus = def.ActionStatusEnd
			actionEndTime = unixTimestamp
			break
		}

		//bdb compacting
		pattern = `\[bitalosdb/(\w+)\] \[(\w+\s+\d+)\] compact bithash start fns.*reserveSize:([\d.]+\w)`
		re = regexp.MustCompile(pattern)
		match = re.FindStringSubmatch(logText)
		if len(match) == 4 {
			dbType = match[1]
			job = match[2]
			actionSize = match[3]
			actionType = def.ActionTypeBdbCompacting
			actionStatus = def.ActionStatusStart
			actionStartTime = unixTimestamp
			break
		}

		//bdb compact end
		pattern = `\[bitalosdb/(\w+)\].*\[(\w+\s+\d+)\] compact bithash files .* success delKey:(\d+) delKeyTotalOld`
		re = regexp.MustCompile(pattern)
		match = re.FindStringSubmatch(logText)
		if len(match) == 4 {
			dbType = match[1]
			job = match[2]
			keyNums, _ = strconv.ParseInt(match[3], 10, 64)
			actionType = def.ActionTypeBdbCompacted
			actionStatus = def.ActionStatusEnd
			actionEndTime = unixTimestamp
			break
		}
		//expire delete
		pattern = `scan delete end delKeys:(\d+) expireKeys:(\d+) zsetKeys:(\d+) cost:(\d+\.\d+)s`
		re = regexp.MustCompile(pattern)
		match = re.FindStringSubmatch(logText)
		if len(match) == 5 {
			actionDuration, _ = strconv.ParseFloat(match[4], 64)
			if actionDuration < 2 {
				return nil
			}
			keyNums, _ = strconv.ParseInt(match[1], 10, 64)
			actionType = def.ActionTypeDelete
			actionStatus = def.ActionStatusEnd
			actionEndTime = unixTimestamp
			break
		}
		//expire db flushed
		pattern = `\[bitable/expire\] \[(\w+\s+\d+)\] flushed \d+ memtable to`
		re = regexp.MustCompile(pattern)
		match = re.FindStringSubmatch(logText)
		if len(match) == 2 {
			dbType = "expire"
			job = match[1]
			actionType = def.ActionTypeBitableMemFlushed
			actionStatus = def.ActionStatusEnd
			actionEndTime = unixTimestamp
			break
		}
		//expire db compacting
		pattern = `\[bitable/expire\] \[(\w+\s+\d+)\] compacting\(`
		re = regexp.MustCompile(pattern)
		match = re.FindStringSubmatch(logText)
		if len(match) == 2 {
			dbType = "expire"
			job = match[1]
			actionType = def.ActionTypeBitableCompacting
			actionStatus = def.ActionStatusDoing
			actionEndTime = unixTimestamp
			break
		}
		//expire db compacted
		pattern = `\[bitable/expire\] \[(\w+\s+\d+)\] compacted\(.*(\d+\.\d+)s total`
		re = regexp.MustCompile(pattern)
		match = re.FindStringSubmatch(logText)
		if len(match) == 3 {
			dbType = "expire"
			job = match[1]
			actionDuration, _ = strconv.ParseFloat(match[2], 64)
			actionType = def.ActionTypeBitableMemCompacted
			actionStatus = def.ActionStatusEnd
			actionEndTime = unixTimestamp
			break
		}
		//raft log flushed
		pattern = `\[bitable/raftlog\] \[(\w+\s+\d+)\] flushed \d+ memtable to.*(\d+\.\d+)s total`
		re = regexp.MustCompile(pattern)
		match = re.FindStringSubmatch(logText)
		if len(match) == 3 {
			dbType = "raftlog"
			job = match[1]
			actionDuration, _ = strconv.ParseFloat(match[2], 64)
			actionType = def.ActionTypeRaftFlushed
			actionStatus = def.ActionStatusEnd
			actionEndTime = unixTimestamp
			break
		}
		//raft compacted
		pattern = `\[bitable/raftlog\] \[(\w+\s+\d+)\] compacted\(.*(\d+\.\d+)s total`
		re = regexp.MustCompile(pattern)
		match = re.FindStringSubmatch(logText)
		if len(match) == 3 {
			dbType = "raftlog"
			job = match[1]
			actionDuration, _ = strconv.ParseFloat(match[2], 64)
			actionType = def.ActionTypeRaftCompacted
			actionStatus = def.ActionStatusEnd
			actionEndTime = unixTimestamp
			break
		}
		//v8 vm flush
		pattern = `flushed \d+ vmtable to bituple done cost:(\d+\.\d+)s`
		re = regexp.MustCompile(pattern)
		match = re.FindStringSubmatch(logText)
		if len(match) == 2 {
			dbType = "vm"
			actionDuration, _ = strconv.ParseFloat(match[1], 64)
			actionType = def.ActionTypeVmFlushed
			actionStatus = def.ActionStatusEnd
			actionEndTime = unixTimestamp
			break
		}
		//v8 vt gc
		pattern = `\[bitalosdb\] \[VTGC \d+\] vt:.*cost:(\d+\.\d+)s`
		re = regexp.MustCompile(pattern)
		match = re.FindStringSubmatch(logText)
		if len(match) == 2 {
			dbType = "vt"
			actionDuration, _ = strconv.ParseFloat(match[1], 64)
			actionType = def.ActionTypeVtGC
			actionStatus = def.ActionStatusEnd
			actionEndTime = unixTimestamp
			break
		}
		//v8 delete key
		pattern = `\[bitalosdb\] \[ELIMINATETASK \d+\].*delKeys:(\d+) cost:(\d+\.\d+)s`
		re = regexp.MustCompile(pattern)
		match = re.FindStringSubmatch(logText)
		if len(match) == 3 {
			if match[2] == "0.000" || match[1] == "0" {
				return nil
			}
			actionDuration, _ = strconv.ParseFloat(match[2], 64)
			keyNums, _ = strconv.ParseInt(match[1], 10, 64)
			actionType = def.ActionTypeV8Delete
			actionStatus = def.ActionStatusEnd
			actionEndTime = unixTimestamp
			break
		}
		//v8 rehash
		pattern = `\[bitalosdb\] \[VTREHASH \d+\].*rehash done.*cost:(\d+\.\d+)s`
		re = regexp.MustCompile(pattern)
		match = re.FindStringSubmatch(logText)
		if len(match) == 2 {
			dbType = "vt"
			actionDuration, _ = strconv.ParseFloat(match[1], 64)
			actionType = def.ActionTypeVtRehash
			actionStatus = def.ActionStatusEnd
			actionEndTime = unixTimestamp
			break
		}
		break
	}
	if len(match) < 2 {
		return nil
	}
	return &QData{
		logLevel:        logLevel,
		actionStartTime: actionStartTime,
		actionEndTime:   actionEndTime,
		rawContent:      logText,
		dbType:          dbType,
		job:             job,
		actionSize:      actionSize,
		keyNums:         keyNums,
		actionDuration:  actionDuration,
		actionStatus:    actionStatus,
		actionType:      actionType,
	}
}
