package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"machine-exporter/collector"
	"machine-exporter/helper"
	"machine-exporter/internal"
	"strconv"
	"strings"
	"sync"

	"github.com/garyburd/redigo/redis"
	"github.com/prometheus/client_golang/prometheus"
)

type Proxy struct {
	wg        sync.WaitGroup
	instances []collector.Instance
	gaugeVec  map[string]collector.ProxyProcessor
	mu        sync.RWMutex
	cmu       sync.RWMutex
	cache     internal.MachinePortMetric
	cache2    internal.MachinePortMetric
}

func NewProxy(instance []collector.Instance) prometheus.Collector {
	return &Proxy{
		wg:        sync.WaitGroup{},
		instances: instance,
		gaugeVec: map[string]collector.ProxyProcessor{
			rUsageCpuName:       newRUsageCpu(),
			rUsageMemName:       newRUsageMem(),
			cdmOpsQpsName:       newCdmOpsQps(),
			cmdCostAvgName:      newCmdCostAvg(),
			cmdCostReadName:     newCmdCostRead(),
			cmdCostWriteName:    newCmdCostWrite(),
			cdmOpsFailsName:     newCdmOpsFails(),
			sessionsAliveName:   newSessionsAlive(),
			poolActiveCountName: newPoolActiveCount(),
			poolIdleCountName:   newPoolIdleCount(),
			cpuThrottledNrName:  newCpuThrottledNr(),
			CmdOpStrQpsName:     newCmdOpStrQps(),
			CmdOpStrCostName:    newCmdOpStrCost(),
			netTotalName:        newNetTotal(),
		}}
}

func (p *Proxy) Describe(desc chan<- *prometheus.Desc) {
	for _, v := range p.gaugeVec {
		v.Describe(desc)
	}
}

func (p *Proxy) Collect(metrics chan<- prometheus.Metric) {
	p.process2()
	for _, v := range p.gaugeVec {
		v.Collect(metrics)
	}
}

func (p *Proxy) process() {
	for _, v := range p.instances {
		p.wg.Add(1)
		go func(i collector.Instance) {
			defer func() {
				p.wg.Done()
				if err := recover(); err != nil {
					fmt.Println(err)
				}
			}()
			info, err := p.info(i)
			if err != nil {
				return
			}
			for k, v := range info {
				if g, ok := p.gaugeVec[k]; ok {
					g.Process(i.IP, i.Name, i.Port, i.Idc, v)
				}
			}
			return
		}(v)
	}
	p.wg.Wait()
}

func (p *Proxy) process2() {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, v := range p.instances {
		metrics := p.getCache(v)
		for k, val := range metrics {
			if g, ok := p.gaugeVec[k]; ok {
				g.Process(v.IP, v.Name, v.Port, v.Idc, val)
			}
		}
		metrics2 := p.getCache2(v)
		for k, val := range metrics2 {
			if strings.Contains(k, CmdOpStrQpsName) {
				if g, ok := p.gaugeVec[CmdOpStrQpsName]; ok {
					cmd := strings.ReplaceAll(k, CmdOpStrQpsName, "")
					g.Process(v.IP, v.Name, v.Port, v.Idc+"_"+cmd, val)
				}
			} else if strings.Contains(k, CmdOpStrCostName) {
				if g, ok := p.gaugeVec[CmdOpStrCostName]; ok {
					cmd := strings.ReplaceAll(k, CmdOpStrCostName, "")
					g.Process(v.IP, v.Name, v.Port, v.Idc+"_"+cmd, val)
				}
			}
		}

	}
}

func (p *Proxy) UpdateCpuLimit() {
	cpuLimitData := make([]internal.NodeCpuLimit, 0, len(p.instances))
	processMachineId := make(map[int]struct{}, 1)
	for _, v := range p.instances {
		if _, ok := processMachineId[v.MachineId]; ok {
			continue
		}

		machineNodes, err := helper.GetMachineCpuLimit(v.MachineId)
		if err != nil {
			processMachineId[v.MachineId] = struct{}{}
			log.Printf("ip:%s get cpu fail. err: %+v\n", v.IP, err)
			continue
		}
		processMachineId[v.MachineId] = struct{}{}

		for _, n := range machineNodes {
			cpuLimitData = append(cpuLimitData, internal.NodeCpuLimit{
				IP:             v.IP,
				Port:           strconv.Itoa(n.ServicePort),
				CpuThrottledNr: float64(n.CpuThrottledNr),
			})
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, n := range cpuLimitData {
		p.updateCacheOneField(n.IP, n.Port, cpuThrottledNrName, n.CpuThrottledNr)
	}
}

func (p *Proxy) CollectInfo() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.cache = nil

	for _, v := range p.instances {
		p.wg.Add(1)
		go func(i collector.Instance) {
			defer func() {
				p.wg.Done()
				if err := recover(); err != nil {
					fmt.Println(err)
				}
			}()
			info, err := p.info(i)
			if err != nil {
				return
			}

			for k := range info {
				if _, ok := p.gaugeVec[k]; !ok {
					delete(info, k)
				}
			}

			p.updateCache(i.IP, i.Port, info)
			return
		}(v)
	}
	p.wg.Wait()
}

func (p *Proxy) CollectInfoStat() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.cache2 = nil
	for _, v := range p.instances {
		p.wg.Add(1)
		go func(i collector.Instance) {
			defer func() {
				p.wg.Done()
				if err := recover(); err != nil {
					fmt.Println(err)
				}
			}()
			proxyStatInfo, err := p.infoProxyStats(i)
			if err != nil {
				return
			}
			p.updateCache2(i.IP, i.Port, proxyStatInfo)
			return
		}(v)
	}
	p.wg.Wait()
}

func (p *Proxy) info(instance collector.Instance) (map[string]float64, error) {
	conn := instance.StoredPool.Get()
	/*
		conn, err := redis.Dial("tcp", instance.IP+":"+instance.Port, redis.DialReadTimeout(time.Second), redis.DialWriteTimeout(time.Second), redis.DialConnectTimeout(time.Second))
		if err != nil {
			return nil, err
		}
	*/
	defer func() {
		_ = conn.Close()
	}()
	do, err := redis.String(conn.Do("info"))
	if err != nil {
		return nil, err
	}
	return collector.ParseInfo(do), nil
}

func (p *Proxy) infoProxyStats(instance collector.Instance) (map[string]float64, error) {
	if instance.Name == "ocr-search" || instance.Name == "ocr-search-inv" || instance.Name == "ocr-search-page" {
		return map[string]float64{}, nil
	}
	statsUrl := fmt.Sprintf("http://%s:%s/proxy/stats", instance.IP, instance.AdminPort)
	res, err := instance.HttpClient.Get(statsUrl)
	if err != nil {
		return nil, err
	}
	body, _ := io.ReadAll(res.Body)
	defer res.Body.Close()
	proxyStatsInfo := collector.ProxyStatsResp{}
	err = json.Unmarshal(body, &proxyStatsInfo)
	if err != nil {
		return nil, err
	}
	if proxyStatsInfo.Status != 200 {
		return nil, errors.New("failed to get statsUrl: " + string(body))
	}
	return collector.ParseProxyStatInfo(proxyStatsInfo), nil
}

func (p *Proxy) updateCache(ip, port string, metrics map[string]float64) {
	p.cmu.Lock()
	defer p.cmu.Unlock()

	if p.cache == nil {
		p.cache = make(map[string]map[string]internal.MetricList)
	}
	if p.cache[ip] == nil {
		p.cache[ip] = make(map[string]internal.MetricList)
	}
	p.cache[ip][port] = metrics
}

func (p *Proxy) updateCache2(ip, port string, metrics map[string]float64) {
	p.cmu.Lock()
	defer p.cmu.Unlock()

	if p.cache2 == nil {
		p.cache2 = make(map[string]map[string]internal.MetricList)
	}
	if p.cache2[ip] == nil {
		p.cache2[ip] = make(map[string]internal.MetricList)
	}
	p.cache2[ip][port] = metrics
}

func (p *Proxy) updateCacheOneField(ip, port string, metricName string, value float64) {
	if _, ok := p.cache[ip]; !ok {
		return
	}
	if _, ok := p.cache[ip][port]; !ok {
		return
	}
	p.cache[ip][port][metricName] = value
}

func (p *Proxy) getCache(instance collector.Instance) map[string]float64 {
	if ipCache, ok := p.cache[instance.IP]; ok {
		if portCache, ok2 := ipCache[instance.Port]; ok2 {
			return portCache
		}
	}
	return nil
}

func (p *Proxy) getCache2(instance collector.Instance) map[string]float64 {
	if ipCache, ok := p.cache2[instance.IP]; ok {
		if portCache, ok2 := ipCache[instance.Port]; ok2 {
			return portCache
		}
	}
	return nil
}
