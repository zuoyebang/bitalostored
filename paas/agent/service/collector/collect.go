package collector

import (
	"encoding/json"
	"fmt"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/config"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/def"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"

	"github.com/nxadm/tail"

	"github.com/zuoyebang/bitalostored/paas/agent/internal/webclient"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
)

type Collector struct {
	antsPool *ants.PoolWithFunc
	PsQueue  *PsQueue
	SQueue   *SQueue
	SaQueue  *SaQueue
	area     string
	CloseCs  []chan struct{}
	mu       sync.Mutex
}

type NodeDeployInfoResp struct {
	webclient.PaaSResponse
	Data *DeployInfo `json:"data"`
}

type ClusterInfo struct {
	Path        string `json:"path"`
	Port        int64  `json:"port"`
	ClusterName string `json:"clusterName"`
}

type DeployInfo struct {
	LocalIp   string         `json:"localIp"`
	IDC       string         `json:"idc"`
	ProxyLog  []*ClusterInfo `json:"proxyLogPath"`
	ServerLog []*ClusterInfo `json:"serverLogPath"`
}

type antsCommandParams struct {
	wg          *sync.WaitGroup
	logFile     string
	logFileType uint8
	closeC      chan struct{}
	PsQueue     *PsQueue
	SQueue      *SQueue
	SaQueue     *SaQueue
	area        string
	Port        int64
	ClusterName string
}

var LocalIpCollector string
var IdcCollector string

func Start(area string) {
	antsPool, _ := ants.NewPoolWithFunc(def.AntsNums, antsCallback, ants.WithExpiryDuration(time.Minute), ants.WithPreAlloc(true))
	var c *Collector
	c = &Collector{
		PsQueue: NewPsQueue(),
		SQueue:  NewSQueue(),
		SaQueue: NewSaQueue(),
		CloseCs: make([]chan struct{}, 0),
	}
	c.area = area
	c.antsPool = antsPool
	for {
		c.Run()
		time.Sleep(time.Second)
	}
}

func (c *Collector) CloseAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.CloseCs) > 0 {
		for _, closeC := range c.CloseCs {
			close(closeC)
		}
		c.CloseCs = make([]chan struct{}, 0)
	}
}

func (c *Collector) addCloseC(closeC chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.CloseCs = append(c.CloseCs, closeC)
}

func (c *Collector) Run() {
	deployInfo, err := GetNodeDeployPath()
	if err != nil {
		return
	}
	if deployInfo.Status != 0 {
		logs.Warn("get deploy path failed stats not 0")
		return
	}
	LocalIpCollector = deployInfo.Data.LocalIp
	IdcCollector = deployInfo.Data.IDC
	wg := sync.WaitGroup{}
	if len(deployInfo.Data.ServerLog) > 0 {
		for _, server := range deployInfo.Data.ServerLog {
			wg.Add(1)
			closeC := make(chan struct{}, 1)
			c.addCloseC(closeC)
			m := &antsCommandParams{
				wg:          &wg,
				logFile:     server.Path,
				logFileType: def.ServerLogType,
				closeC:      closeC,
				SQueue:      c.SQueue,
				SaQueue:     c.SaQueue,
				area:        c.area,
				Port:        server.Port,
				ClusterName: server.ClusterName,
			}
			err := c.antsPool.Invoke(m)
			if err != nil {
				wg.Done()
				logs.Warnf("ants pool invoke failed, current running file: %s, err: %s", server.Path, err.Error())
			}
		}
	}

	if len(deployInfo.Data.ProxyLog) > 0 {
		for _, proxy := range deployInfo.Data.ProxyLog {
			wg.Add(1)
			closeC := make(chan struct{}, 1)
			c.addCloseC(closeC)
			m := &antsCommandParams{
				wg:          &wg,
				logFile:     proxy.Path,
				logFileType: def.ProxyLogType,
				closeC:      closeC,
				PsQueue:     c.PsQueue,
				Port:        proxy.Port,
				ClusterName: proxy.ClusterName,
			}
			err := c.antsPool.Invoke(m)
			if err != nil {
				wg.Done()
				logs.Warnf("ants pool invoke failed, current running file: %s, err: %s", proxy.Path, err.Error())
			}
		}
	}

	if len(deployInfo.Data.ServerLog) > 0 {
		go func() {
			wg.Add(1)
			defer wg.Done()
			t := time.Now()
			fileDay := t.Format("20060102")
			fileDate := t.Format("2006010215")
			closeC := make(chan struct{}, 1)
			c.addCloseC(closeC)
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					t := time.Now()
					date := t.Format("20060102")
					h := t.Format("2006010215")
					if date != fileDay {
						fileDay = date
						fileDate = h
						logs.Info("collect vt day changed, continue")
					}
					if h != fileDate {
						collectVtSize(deployInfo)
						fileDate = h
					}
				case <-closeC:
					logs.Info("collect vt finish")
					return
				}
			}
		}()
	}
	wg.Wait()
	logs.Info("all collect run finish")
}

func Command(key string, arg ...string) (string, error) {
	cmd := exec.Command(key, arg...)
	b, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(b)), err
}

func GetNodeDeployPath() (*NodeDeployInfoResp, error) {
	//http://127.0.0.1/storedpaas/machine/nodedeployinfo?machineId=
	p := fmt.Sprintf("%s?machineId=%d", config.GetPaaSAddress(def.URL_DEPLOY), config.C.MachineId)
	netClient := &http.Client{
		Timeout: def.HttpTimeout,
	}
	res, err := netClient.Get(p)
	if err != nil {
		logs.Warn("get deploy path", err)
		return nil, err
	}
	resp := &NodeDeployInfoResp{}
	if err = json.NewDecoder(res.Body).Decode(resp); err != nil {
		logs.Warn("get deploy path", err, p)
		return nil, err
	}
	return resp, nil
}

func antsCallback(params interface{}) {
	m, _ := params.(*antsCommandParams)
	if m.wg != nil {
		defer m.wg.Done()
	}

	if m.logFileType == def.ProxyLogType {
		proxyAntsCallback(m)
	}
	if m.logFileType == def.ServerLogType {
		serverAntsCallback(m)
	}
}

func serverAntsCallback(m *antsCommandParams) {
	logs.Infof("begin tail new server file:%s", m.logFile)

	for {
		t := time.Now()
		fileDay := t.Format("20060102")
		slowLogFile := fmt.Sprintf("%s/bitalosdb/log/%s%s", m.logFile, "bitalos.log.slow.", fileDay)
		errLogFile := fmt.Sprintf("%s/bitalosdb/log/%s%s", m.logFile, "bitalos.log.err.", fileDay)
		serverLogFile := fmt.Sprintf("%s/bitalosdb/log/%s%s", m.logFile, "bitalos.log.", fileDay)
		supervisordLog := fmt.Sprintf("%s/bitalos-server.dump.0", m.logFile)

		slowFile, err := tail.TailFile(slowLogFile, tail.Config{
			Follow:   true,
			Poll:     true,
			ReOpen:   true,
			Location: &tail.SeekInfo{Offset: 0, Whence: checkFileExist(slowLogFile)},
			Logger:   tail.DiscardingLogger,
		})
		if err != nil {
			logs.Warnf("tail slow log file %s failed: %v", slowLogFile, err)
			time.Sleep(time.Second)
			continue
		}

		errFile, err := tail.TailFile(errLogFile, tail.Config{
			Follow:   true,
			Poll:     true,
			ReOpen:   true,
			Location: &tail.SeekInfo{Offset: 0, Whence: checkFileExist(errLogFile)},
			Logger:   tail.DiscardingLogger,
		})
		if err != nil {
			logs.Warnf("tail err log file %s failed: %v", errLogFile, err)
			_ = slowFile.Stop()
			time.Sleep(time.Second)
			continue
		}

		superFile, err := tail.TailFile(supervisordLog, tail.Config{
			Follow:   true,
			Poll:     true,
			ReOpen:   true,
			Location: &tail.SeekInfo{Offset: 0, Whence: checkFileExist(supervisordLog)},
			Logger:   tail.DiscardingLogger,
		})
		if err != nil {
			logs.Warnf("tail supervisord log file %s failed: %v", supervisordLog, err)
			_ = slowFile.Stop()
			_ = errFile.Stop()
			time.Sleep(time.Second)
			continue
		}

		serverFile, err := tail.TailFile(serverLogFile, tail.Config{
			Follow:   true,
			Poll:     true,
			ReOpen:   true,
			Location: &tail.SeekInfo{Offset: 0, Whence: checkFileExist(serverLogFile)},
			Logger:   tail.DiscardingLogger,
		})
		if err != nil {
			logs.Warnf("tail server log file %s failed: %v", serverLogFile, err)
			_ = slowFile.Stop()
			_ = errFile.Stop()
			_ = superFile.Stop()
			time.Sleep(time.Second)
			continue
		}

		ticker := time.NewTicker(10 * time.Second)
		shouldReopen := false

		for {
			select {
			case line := <-slowFile.Lines:
				if line == nil {
					continue
				}
				if m.logFileType == def.ProxyLogType {
					m.PsQueue.HandleProxySlowLog(line.Text, m.ClusterName, m.Port)
				}
				if m.logFileType == def.ServerLogType {
					m.SQueue.HandleServerSlowLog(line.Text, m.ClusterName, m.Port)
				}

			case line := <-errFile.Lines:
				if line == nil {
					continue
				}
				if m.logFileType == def.ProxyLogType {
					m.PsQueue.HandleProxyErrLog(line.Text, m.ClusterName, m.Port)
				}
				if m.logFileType == def.ServerLogType {
					m.SQueue.HandleServerErrLog(line.Text, m.ClusterName, m.Port)
				}
			case line := <-serverFile.Lines:
				if line == nil {
					continue
				}
				m.SaQueue.HandleServerActionLog(line.Text, m.ClusterName, m.Port)
			case line := <-superFile.Lines:
				if line == nil {
					continue
				}
				m.SQueue.HandleServerSupervisorLog(line.Text, m.ClusterName, m.Port)
			case <-m.closeC:
				logs.Infof("server logFile:%s receive close signal, bye", m.logFile)
				_ = slowFile.Stop()
				_ = errFile.Stop()
				_ = superFile.Stop()
				_ = serverFile.Stop()
				ticker.Stop()
				return
			case <-ticker.C:
				t := time.Now()
				date := t.Format("20060102")
				if fileDay != date {
					_ = slowFile.Stop()
					_ = errFile.Stop()
					_ = superFile.Stop()
					_ = serverFile.Stop()
					ticker.Stop()
					shouldReopen = true
					break
				}
			}
			if shouldReopen {
				break
			}
		}
	}
}

func proxyAntsCallback(m *antsCommandParams) {
	logs.Infof("begin tail new proxy file:%s", m.logFile)

	for {
		t := time.Now()
		fileDate := t.Format("2006010215")
		fileDay := t.Format("20060102")
		slowLogFile := fmt.Sprintf("%s/%s%s", m.logFile, "proxy.slow.log.", fileDate)
		errLogFile := fmt.Sprintf("%s/%s%s", m.logFile, "stored-proxy.log.", fileDate)

		slowFile, err := tail.TailFile(slowLogFile, tail.Config{
			Follow:   true,
			Poll:     true,
			ReOpen:   true,
			Location: &tail.SeekInfo{Offset: 0, Whence: checkFileExist(slowLogFile)},
			Logger:   tail.DiscardingLogger,
		})
		if err != nil {
			logs.Warnf("tail slow log file %s failed: %v", slowLogFile, err)
			time.Sleep(time.Second)
			continue
		}

		errFile, err := tail.TailFile(errLogFile, tail.Config{
			Follow:   true,
			Poll:     true,
			ReOpen:   true,
			Location: &tail.SeekInfo{Offset: 0, Whence: checkFileExist(errLogFile)},
			Logger:   tail.DiscardingLogger,
		})
		if err != nil {
			logs.Warnf("tail err log file %s failed: %v", errLogFile, err)
			_ = slowFile.Stop()
			time.Sleep(time.Second)
			continue
		}

		ticker := time.NewTicker(10 * time.Second)
		shouldReopen := false

		for {
			select {
			case line := <-slowFile.Lines:
				if line == nil {
					continue
				}
				if m.logFileType == def.ProxyLogType {
					m.PsQueue.HandleProxySlowLog(line.Text, m.ClusterName, m.Port)
				}
				if m.logFileType == def.ServerLogType {
					m.SQueue.HandleServerSlowLog(line.Text, m.ClusterName, m.Port)
				}

			case line := <-errFile.Lines:
				if line == nil {
					continue
				}
				if m.logFileType == def.ProxyLogType {
					m.PsQueue.HandleProxyErrLog(line.Text, m.ClusterName, m.Port)
				}
				if m.logFileType == def.ServerLogType {
					m.SQueue.HandleServerErrLog(line.Text, m.ClusterName, m.Port)
				}
			case <-m.closeC:
				logs.Infof("proxy logFile:%s receive close signal, bye", m.logFile)
				_ = slowFile.Stop()
				_ = errFile.Stop()
				ticker.Stop()
				return
			case <-ticker.C:
				t := time.Now()
				date := t.Format("2006010215")
				day := t.Format("20060102")
				if fileDay != day || fileDate != date {
					_ = slowFile.Stop()
					_ = errFile.Stop()
					ticker.Stop()
					logs.Infof("proxy logFile:%s date changed, stop tailing, reopen file", m.logFile)
					shouldReopen = true
					break
				}
			}
			if shouldReopen {
				break
			}
		}
	}
}

func checkFileExist(filePath string) int {
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return io.SeekStart
	}
	return io.SeekEnd
}
