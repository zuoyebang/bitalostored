package collector

import (
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/agent/dao/collect"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/def"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"runtime"
	"sync"
	"time"
)

type IQueue interface {
	GetQchan() chan *QData
	GetQuitC() chan struct{}
	InsertLog([]*QData)
	Close()
	Push(data *QData)
	SourceType() string
}

type QData struct {
	rawContent      string
	exceptionInfo   string
	logType         uint8
	logLevel        uint8
	logTime         int64
	duration        int64
	ip              string
	dstIp           string
	port            int64
	dstPort         int64
	gid             int64
	slotId          int64
	key             string
	command         string
	clusterName     string
	actionType      uint8
	dbType          string
	actionSize      string
	keyNums         int64
	actionStartTime int64
	actionStatus    uint8
	actionEndTime   int64
	actionDuration  float64
	job             string
	params          string
}

const ChanLen = 2000
const FlushDbLen = 500
const Frequency = 10 * time.Second

func StartConsume(q IQueue) {
	go func(q IQueue) {
		defer func() {
			if e := recover(); e != nil {
				buf := make([]byte, 2048)
				n := runtime.Stack(buf, false)
				buf = buf[0:n]
				logs.Errorf("queue run panic, [err:%v] [panic:%v]", e, string(buf))
				time.Sleep(100 * time.Millisecond)
				StartConsume(q)
			}
		}()
		qchan := q.GetQchan()
		source := q.SourceType()
		ticker := time.NewTicker(Frequency)
		var qdatas []*QData
		defer ticker.Stop()
		quitC := q.GetQuitC()
		logs.Infof("start consume loop, source=%s", source)
		for {
			select {
			case <-ticker.C:
				//logs.Infof("%s time is up, data length=%d", source, len(qdatas))
				if len(qdatas) <= 0 {
					break
				}
				q.InsertLog(qdatas)
				qdatas = qdatas[0:0]
			case <-quitC:
				return
			case qdata, ok := <-qchan:
				if !ok {
					logs.Infof("receive close chan, close queue chan")
					return
				}
				//logs.Infof("%s receive one data, qdata:%s", source, qdata.rawContent)
				qdatas = append(qdatas, qdata)
				if len(qdatas) >= FlushDbLen {
					//logs.Info("length exceed")
					q.InsertLog(qdatas)
					qdatas = qdatas[0:0]
				}
			}
		}
	}(q)
}

type safeServerActionMap struct {
	sync.RWMutex
	data map[string]*safeInnerMap //node
}

type safeInnerMap struct {
	sync.Mutex
	innerData map[string]*serverActionLog //event_dbtype_info
}

type serverActionLog struct {
	qd       *QData
	insertId int64
	hasFlush bool
}

var serverActionMap = &safeServerActionMap{
	data: make(map[string]*safeInnerMap),
}

func StartConsumeServerAction(q IQueue) {
	go func(q IQueue) {
		qchan := q.GetQchan()
		source := q.SourceType()
		logs.Infof("start consume loop, source=%s", source)
		for {
			select {
			case qdata, ok := <-qchan:
				if !ok {
					time.Sleep(2 * time.Second)
					continue
				}
				node := fmt.Sprintf("%s:%d", qdata.ip, qdata.port)
				uniqKey := getUniqKey(qdata)
				logs.Infof("receive server action data, node:%v uniqKey:%v actionStatus:%v", node, uniqKey, qdata.actionStatus)
				serverActionMap.Lock()
				if _, ok := serverActionMap.data[node]; !ok {
					serverActionMap.data[node] = &safeInnerMap{innerData: make(map[string]*serverActionLog)}
				}
				serverActionMap.Unlock()
				if qdata.actionStatus == def.ActionStatusStart {
					logs.Infof("receive server action start data, uniqKey:%v", uniqKey)
					serverActionMap.data[node].Lock()
					if _, ok := serverActionMap.data[node].innerData[uniqKey]; !ok {
						serverActionMap.data[node].innerData[uniqKey] = &serverActionLog{qd: qdata}
					}
					serverActionMap.data[node].Unlock()
				}
				if qdata.actionStatus == def.ActionStatusEnd {
					logs.Infof("receive server action end data, uniqKey:%v node:%v", uniqKey, node)
					serverActionMap.data[node].Lock()
					if _, ok := serverActionMap.data[node].innerData[uniqKey]; !ok {
						logs.Info("no start data")
						durationMs := qdata.actionDuration * 1000
						qdata.actionStartTime = qdata.actionEndTime - int64(durationMs)
						_, _ = insertServerActionLog(qdata)
					} else {
						insertId := serverActionMap.data[node].innerData[uniqKey].insertId
						logs.Infof("receive server action end data, uniqKey:%v insertId:%v", uniqKey, insertId)
						if insertId <= 0 {
							qdata.actionStartTime = serverActionMap.data[node].innerData[uniqKey].qd.actionStartTime
							_, _ = insertServerActionLog(qdata)
						} else {
							_ = updateServerActionLog(insertId, qdata)
						}
						delete(serverActionMap.data[node].innerData, uniqKey)
					}
					serverActionMap.data[node].Unlock()
					serverActionMap.Lock()
					if len(serverActionMap.data[node].innerData) <= 0 {
						delete(serverActionMap.data, node)
					}
					serverActionMap.Unlock()
				}
			case <-q.GetQuitC():
				return
			}
		}
	}(q)
}

func getUniqKey(qdata *QData) string {
	return fmt.Sprintf("%d_%s_%s", qdata.actionType, qdata.dbType, qdata.job)
}

func insertServerActionLog(qdata *QData) (int64, error) {
	d := &collect.ServerActionLogModel{
		Ip:              qdata.ip,
		Port:            qdata.port,
		ClusterName:     qdata.clusterName,
		ActionType:      qdata.actionType,
		DbType:          qdata.dbType,
		ActionSize:      qdata.actionSize,
		KeyNums:         qdata.keyNums,
		ActionStartTime: qdata.actionStartTime,
		ActionEndTime:   qdata.actionEndTime,
		Duration:        qdata.actionDuration,
		Job:             qdata.job,
		RawContent:      qdata.rawContent,
		CreateTime:      time.Now().Unix(),
	}
	serverActionLogModel := collect.NewServerActionLogService()
	err := serverActionLogModel.InsertLog(d)
	if err != nil {
		return 0, err
	}
	return d.Id, nil
}

func updateServerActionLog(id int64, qdata *QData) error {
	d := &collect.ServerActionLogModel{
		Id:              id,
		Ip:              qdata.ip,
		Port:            qdata.port,
		ClusterName:     qdata.clusterName,
		ActionType:      qdata.actionType,
		DbType:          qdata.dbType,
		ActionSize:      qdata.actionSize,
		KeyNums:         qdata.keyNums,
		ActionStartTime: qdata.actionStartTime,
		ActionEndTime:   qdata.actionEndTime,
		Duration:        qdata.actionDuration,
		Job:             qdata.job,
		RawContent:      qdata.rawContent,
		UpdateTime:      time.Now().Unix(),
	}
	serverActionLogModel := collect.NewServerActionLogService()
	err := serverActionLogModel.UpdateLog(d)
	return err
}

func flushServerStartLog() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				serverActionMap.RLock()
				if len(serverActionMap.data) <= 0 {
					serverActionMap.RUnlock()
					logs.Info("server action map is empty")
					continue
				}
				logs.Infof("server action map length:%d", len(serverActionMap.data))
				now := time.Now().UnixMilli()
				for node, eventInfo := range serverActionMap.data {
					for event, log := range eventInfo.innerData {
						if log.qd.actionStartTime > 0 && now-log.qd.actionStartTime > 5000 {
							eventInfo.Lock()
							if serverActionMap.data[node].innerData[event].hasFlush {
								eventInfo.Unlock()
								continue
							}
							id, err := insertServerActionLog(log.qd)
							logs.Infof("flush server action log, id:%v", id)
							if err == nil {
								serverActionMap.data[node].innerData[event].insertId = id
								serverActionMap.data[node].innerData[event].hasFlush = true
							}
							eventInfo.Unlock()
						}
					}
				}
				serverActionMap.RUnlock()
			}
		}
	}()
}
