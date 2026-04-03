package collector

import (
	"fmt"
	"github.com/nxadm/tail"
	"github.com/panjf2000/ants/v2"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/config"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/def"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Println("Error getting current file path")
		os.Exit(1)
	}
	currentDir := filepath.Dir(currentFile)
	parentDir := filepath.Dir(currentDir)
	err := config.SetConfig(parentDir + "/conf/config_test.toml")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	//connector.InitMysql()
	logs.NewLogger(&logs.Options{
		IsDebug:      false,
		RotationTime: "Daily",
		LogPath:      parentDir + "/log/storedpaas/agent.log",
	})
	m.Run()
}

func TestTailNotExistFile(t *testing.T) {
	fileName := "./test.log"
	slowFile, _ := tail.TailFile(fileName, tail.Config{
		Follow:   true,
		Poll:     false,
		ReOpen:   true,
		Location: &tail.SeekInfo{Offset: 0, Whence: io.SeekEnd},
		Logger:   tail.DiscardingLogger,
	})
	for {
		select {
		case line := <-slowFile.Lines:
			fmt.Println(line)
		}
	}
}

func TestTail(t *testing.T) {
	params := &antsCommandParams{
		logFile:     "/Users/liufang01/Downloads/test",
		logFileType: def.ProxyLogType,
		closeC:      make(chan struct{}, 1),
	}
	go func() {
		fmt.Println("after 1 minute close")
		time.Sleep(time.Minute)
		close(params.closeC)
	}()
	antsCallback(params)
}

func TestQueue(t *testing.T) {
	data := &QData{
		rawContent:  "lskjdflksjdfkjsdlfjsf",
		logTime:     1682654182,
		logType:     1,
		duration:    200635,
		ip:          "10.109.47.208",
		port:        37942,
		key:         "di:629917979",
		command:     "MGET",
		clusterName: "ocr-search",
	}
	//test length
	go func(data *QData) {
		psQueue := NewPsQueue()
		for i := 0; i < 10; i++ {
			psQueue.Push(data)
		}
	}(data)
	//test time
	go func(data *QData) {
		sQueue := NewSQueue()
		for i := 0; i < 10; i++ {
			sQueue.Push(data)
			if i%(FlushDbLen-1) == 0 {
				time.Sleep(Frequency + 1*time.Second)
			}
		}
	}(data)
	time.Sleep(time.Minute)
}

func TestServerActionMemtableFlush(t *testing.T) {
	sa := NewSaQueue()
	t.Log("write start log")
	logText := "2024-09-18 20:00:38.558807 v2@v2.6.15/event.go:196 [INFO] [bitalosdb/hashdata] [BITOWER 7] flushing 1 memtable to bitree"
	sa.HandleServerActionLog(logText, "test", 8080)
	time.Sleep(6 * time.Second)
	t.Log("write end log")
	logText = "2024-09-18 20:00:39.285151 v2@v2.6.15/event.go:199 [INFO] [bitalosdb/hashdata] [BITOWER 7] flushed 1 memtable to bitree iterated(117 M) written(86 M) keys(323599) keysPdKind(168) pdNum(0), in 0.726s, output rate 118 M/s"
	sa.HandleServerActionLog(logText, "test", 8080)
	time.Sleep(10 * time.Second)
}

func TestServerActionCompact(t *testing.T) {
	sa := NewSaQueue()
	t.Log("write compact start log")
	logText := "2024-08-15 02:01:28.396704 v2@v2.6.15/bitree_compaction.go:112 [INFO] [bitalosdb/hashdata] [JOB 71110] compact bithash start"
	sa.HandleServerActionLog(logText, "test", 8080)
	logText = "2024-08-15 02:01:29.040070 bithash/compact.go:106 [INFO] [bitalosdb/hashdata] [COMPACTBITHASH 0] checkFilesMiniSize fileNum=53508 state=IMMUTABLE keyNum=0 conflictKeyNum=0 delKeyNum=0 fileSize:458.171KB"
	sa.HandleServerActionLog(logText, "test", 8080)
	logText = "2024-08-15 02:03:39.513098 v2@v2.6.15/bitree_compaction.go:114 [INFO] [bitalosdb/hashdata] [JOB 71110] compact bithash done"
	sa.HandleServerActionLog(logText, "test", 8080)
	time.Sleep(10 * time.Second)
}

func TestParseProxySlowLog(t *testing.T) {
	logText := "2024-11-26 11:10:02.611814 runtime/asm_amd64.s:1598 [INFO] [ip_port:10.109.4.10:36122] [duration(us):79030] [status:OK] [query:\"MGET di:1497560242 di:1533729351 di:1995130265 di:1033966935 di:1140089594 di:1158046071 di:797238815 di:1021546207 di:1143310673 di:2005892291 di:1050787572 di:1127576275 di:1165124263 di:1261954536 di:2003854802 di:1009039298 di:1162953124 di:1424270336 \"]"
	data := parseProxySlowLog(logText)
	t.Logf("data=%+v", data)
}

func TestParseProxyErrLog(t *testing.T) {
	//logText = "2024-10-04 19:07:22.297299 proxy.go:42: [WARN] get info fail, addr : 10.33.39.187:18604, err : read tcp 10.33.39.128:42672->10.33.39.187:18604: i/o timeout"
	logText := "2024-10-04 19:07:22.297299 redis_do.go:118: [WARN] do redis cmd fail addr:10.106.224.94:18696 slotId:453 commandName:DEL args:[] err:invalid key size"
	data := parseProxyErrLog(logText)
	t.Logf("data=%+v", data)
}

func TestParseProxyPanicLog(t *testing.T) {
	logText := "2024-10-04 19:07:22.297299 redis_do.go:118: [WARN] [panic] get group circuit breaker fail groupId"
	data := parseProxyErrLog(logText)
	t.Logf("data=%+v", data)
}

func TestParseProxyIOTimeoutLog(t *testing.T) {
	logText := "2024-10-04 19:07:22.297299 gobreaker@v0.4.1/gobreaker.go:211 [WARN] do redis cmd fail addr:10.106.224.44:18712 slotId:36 commandName:MGET args:[di:1476929527 di:696527322 di:709568610] err:read tcp 10.106.224.54:34458->10.106.224.44:18712: i/o timeout"
	data := parseProxyErrLog(logText)
	t.Logf("data=%+v", data)
}

func TestParseMasterChangeLog(t *testing.T) {
	logText := "2024-11-12 18:26:42.185351 router/probe.go:56 [WARN] groupId:1 slotId:502 master change 10.116.48.129:22707 to 10.116.48.58:22707"
	data := parseProxyErrLog(logText)
	t.Logf("data=%+v", data)
}

func TestParseServerSlowLog(t *testing.T) {
	logText := "2023/05/11 16:35:54 2023-05-08 16:52:44.172342 S | 52744 bitalos-server/server/client.go:259 [ip_port:127.0.0.1:51686] [duration(us):700194] [raftsync(us):0] [quer1y:\"freememory\"] [status:OK]"
	logText = "2025-04-11 16:01:54.712983 raft/queue.go:137 [SLOW] [ip_port:] [duration(us):32551] [raftsync(us):0] [query:\"zremrangebyscore huixuexi:passport:uid_zjxuss_set_100001044502 -inf 1744358514\"] [status:OK]\n"
	data := parseServerSlowLog(logText)
	t.Logf("data=%+v", data)
}

func TestParseServerErrLog(t *testing.T) {
	logText := "2023-05-08 14:42:49.777259 E | 52744 bitalos-server/server/proc.go:68 ^[[31mclient run panic runtime error: invalid memory address or nil pointer dereference:goroutine 893 [running]:"
	logText = "2023-07-04 14:03:01.030470 E | 35323 internal/transport/transport.go:431 Nodehost 10.106.224.143:18869 failed to get a connection to 10.106.224.103:18808, dial tcp 10.106.224.103:18808: i/o timeout"
	logText = "2023-07-21 09:57:56.099711 E | 24469 v2@v2.4.48/bitpage/compaction.go:19 ^[[31m[bitalosdb/zsetindex] [BITPAGEFLUSH] index:0 taskId:1 pn:31 flush panic [err=runtime error: slice bounds out of range [4294967194:333:]] [stack=goroutine 38 [running]:"
	logText = "2023-08-10 00:17:21.023160 E | 43149 src/runtime/asm_amd64.s:1598 handleRequest error:invalid field size"
	logText = "2023-08-09 22:57:45.330438 E | 43149 src/runtime/asm_amd64.s:1598 ^[[31mhandleRequest error:WRONGTYPE Operation against a key holding the wrong kind of value^[[0m\n"
	logText = "2023-08-15 23:10:57.757166 E | 6994 internal/transport/transport.go:431 Nodehost 10.106.224.118:18860 failed to get a connection to 10.33.39.113:18911, dial tcp 10.33.39.113:18911: connect: connection refused"
	//logText = "2023-08-15 17:20:53.001059 E | 28866 src/runtime/asm_amd64.s:1598 ^[[31mhandleRequest error:invalid key size^[[0m"
	data := parseServerErrLog(logText)
	t.Logf("data=%+v", data)
}

func TestParseBdbMemtableFlush(t *testing.T) {
	logText := "2024-08-15 17:00:38.558807 v2@v2.6.15/event.go:196 [INFO] [bitalosdb/hashdata] [BITOWER 7] flushing 1 memtable to bitree"
	data := parseServerActionLog(logText)
	t.Logf("bdb memtable flushing data=%+v", data)
	logText = "2024-11-12 18:40:55.228072 v2@v2.6.15/event.go:199 [INFO] [bitalosdb/meta] [BITOWER 0] flushed 1 memtable to bitree iterated(7.2 M) written(4.1 M) keys(4285) keysPdKind(0) pdNum(0), in 0.023s, output rate 179 M/s"
	data = parseServerActionLog(logText)
	t.Logf("bdb memtable flush end data=%+v", data)
}

func TestParseDelete(t *testing.T) {
	logText := "2024-08-15 17:02:04.625419 engine/bitalos.go:203 [INFO] [DELEXPIRE 14517] scan delete end delKeys:124 expireKeys:124 zsetKeys:0 cost:0.191s"
	data := parseServerActionLog(logText)
	t.Logf("delete data=%+v", data)
}

func TestParseBdbCompact(t *testing.T) {
	log := "2024-11-07 03:07:46.579194 bitree/bithash.go:79 [INFO] [bitalosdb/meta] [COMPACTBITHASH 3] compact bithash start fns:[69762] reserveSize:253.054MB"
	data := parseServerActionLog(log)
	t.Logf("bdb compact start data=%+v", data)
	log = "2024-11-07 03:07:46.579174 bitree/bithash.go:266 [INFO] [bitalosdb/meta] [COMPACTBITHASH 3] compact bithash files [69749 69707 69759] to 69982 success delKey:52084 delKeyTotalOld:2388674 delKeyTotalNew:2336590"
	data = parseServerActionLog(log)
	t.Logf("bdb compact done data=%+v", data)
}

func TestParseSupervisorLog(t *testing.T) {
	logText := "panic: init global config err, please check dbconfig.toml, err : toml: cannot load TOML value of type int64 into a Go float"
	data := parseSupervisorLog(logText)
	t.Logf("data=%+v", data)
}

func TestParseRaftLogCompact(t *testing.T) {
	log := "2025-02-25 23:00:00.007583 logger/logger.go:120 [INFO] [bitable/raftlog] [JOB 60512] compacted(default) L5 [062582 062583 062584] (682 M) + L6 [062476] (155 M) -> L6 [062585 062586] (658 M), in 9.7s (9.7s total), output rate 68 M/s"
	data := parseServerActionLog(log)
	t.Logf("compact start data=%+v", data)
}

func TestParseRaftLogFlush(t *testing.T) {
	log := "2025-02-25 23:01:19.169899 logger/logger.go:120 [INFO] [bitable/raftlog] [JOB 60516] flushed 1 memtable to L0 [062590] (27 M), in 0.2s (0.2s total), output rate 120 M/s"
	data := parseServerActionLog(log)
	t.Logf("raft compact start data=%+v", data)
}

func TestParseExpiredbFlush(t *testing.T) {
	log := "2025-02-25 23:00:50.990767 bitable@v1.3.5/event.go:571 [INFO] [bitable/expire] [JOB 48028] flushing 1 memtable to L0"
	data := parseServerActionLog(log)
	t.Logf("expire flushing data=%+v", data)
	log = "2025-02-25 23:00:51.395133 bitable@v1.3.5/event.go:574 [INFO] [bitable/expire] [JOB 48028] flushed 1 memtable to L0 [045595] (28 M), in 0.4s (0.4s total), output rate 69 M/s"
	data = parseServerActionLog(log)
	t.Logf("expire flushed data=%+v", data)
}

func TestParseExpiredbCompact(t *testing.T) {
	log := "2025-02-25 23:02:03.116366 bitable@v1.3.5/event.go:562 [INFO] [bitable/expire] [JOB 48031] compacting(default) L0 [045581 045582 045583 045584 045585 045586 045587 045588 045589 045590 045591 045592 045593 045594 045595 045596] (443 M) + L5 [045562 045564 045565 045567 045568] (442 M)"
	data := parseServerActionLog(log)
	t.Logf("expire compacting data=%+v", data)
	log = "2025-02-25 23:02:15.829199 bitable@v1.3.5/event.go:565 [INFO] [bitable/expire] [JOB 48031] compacted(default) L0 [045581 045582 045583 045584 045585 045586 045587 045588 045589 045590 045591 045592 045593 045594 045595 045596] (443 M) + L5 [045562 045564 045565 045567 045568] (442 M) -> L5 [045597 045598 045599 045600 045601 045602 045603 045604 045605 045606] (870 M), in 12.7s (12.7s total), output rate 68 M/s"
	data = parseServerActionLog(log)
	t.Logf("expire compacted data=%+v", data)
}

func TestParseVmFlush(t *testing.T) {
	log := "2025-07-14 06:49:20.870373 bitalosdb-v8@v1.2.7/vm_flush.go:145 [INFO] [bitalosdb] [VMSHARD 5] flushed 1 vmtable to bituple done cost:28.018s"
	data := parseServerActionLog(log)
	t.Logf("vm flush data=%v", data)
}

func TestParseVtGC(t *testing.T) {
	log := "2025-07-14 01:03:03.656388 vectortable/vt.go:357 [INFO] [bitalosdb] [VTGC 33] vt:0 oldKeys:675207 oldKDataSize:32 MB oldVDataSize:16 MB newKeys:622201 newKDataSize:29 MB newVDataSize:2.9 MB delKeys:0 expKeys:53006 exKSize:3092156 exVSize:13307513 wtKSize:29466387 wtVSize:2990171 wtCount:map[1:620796 4:21 5:334 6:1050] cost:1.607s"
	data := parseServerActionLog(log)
	t.Logf("vt gc data=%v", data)
}

func TestParseV8Delete(t *testing.T) {
	log := "2025-07-14 00:04:12.627270 bitalosdb-v8@v1.2.8/eliminate.go:182 [INFO] [bitalosdb] [ELIMINATETASK 663] scan end nextScanTs:1752422700 delKeys:0 cost:0.000s"
	log = "2025-07-29 15:46:23.300835 bitalosdb-v8@v1.2.7/eliminate.go:182 [INFO] [bitalosdb] [ELIMINATETASK 7508] scan end nextScanTs:1753775400 delKeys:527 cost:0.003s"
	data := parseServerActionLog(log)
	t.Logf("vm flush data=%v", data)
}

func TestParseVtRehash(t *testing.T) {
	log := "2025-08-04 17:05:16.181065 vectortable/vectortable.go:367 [INFO] [bitalosdb] [VTREHASH 121] vt:0 rehash done oldSize:3906250 newSize:4882813 cost:35.789s"
	data := parseServerActionLog(log)
	t.Logf("vt rehash data=%v", data)
}

func TestRestart(t *testing.T) {
	dir, _ := os.Getwd()
	parentDir := path.Dir(dir)
	err := config.SetConfig(parentDir + "/conf/config_249.toml")
	if err != nil {
		t.Errorf("config load err:%v", err)
		return
	}
	antsPool, _ := ants.NewPoolWithFunc(def.AntsNums, antsCallback, ants.WithExpiryDuration(time.Minute), ants.WithPreAlloc(true))
	c := &Collector{
		PsQueue: NewPsQueue(),
		SQueue:  NewSQueue(),
		CloseCs: make([]chan struct{}, 0),
	}
	c.antsPool = antsPool
	go func() {
		for i := 0; i < 60; i++ {
			time.Sleep(10 * time.Second)
			t.Logf("closecs:%d", len(c.CloseCs))
			if len(c.CloseCs) > 0 {
				for _, closeC := range c.CloseCs {
					close(closeC)
				}
				break
			}
		}
	}()
	c.Run()
}
