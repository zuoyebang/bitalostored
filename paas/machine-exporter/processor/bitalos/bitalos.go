package bitalos

import (
	"fmt"
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

type Bitalos struct {
	wg        sync.WaitGroup
	instances []collector.Instance
	gaugeVec  map[string]collector.ServerProcessor
	mu        sync.RWMutex
	wmu       sync.RWMutex
	cache     map[string]map[string]internal.MetricList
}

func NewBitalos(instance []collector.Instance) prometheus.Collector {
	return &Bitalos{
		wg:        sync.WaitGroup{},
		instances: instance,
		gaugeVec: map[string]collector.ServerProcessor{
			"instantaneous_ops_per_sec":      newInstantaneousOpsPerSec(),
			"memory_total":                   newMemoryTotal(),
			"memory_shr":                     newMemoryShared(),
			"cpu":                            newCpu(),
			"connected_clients":              newConnectedClients(),
			"sync_queue_length":              newSyncQueueLength(),
			"disk_data_human_size":           newDiskDataHumanSize(),
			"disk_used_human_size":           newDiskUsedHumanSize(),
			"disk_used_size":                 newDiskUsedSize(),
			"disk_data_size":                 newDiskDataSize(),
			"disk_raft_nodehost_size":        newDiskRaftNodehostSize(),
			"hash_data_disk_size":            newHashDataDiskSize(),
			"hash_meta_disk_size":            newHashMetaDiskSize(),
			"list_data_disk_size":            newListDataDiskSize(),
			"list_meta_disk_size":            newListMetaDiskSize(),
			"set_data_disk_size":             newSetDataDiskSize(),
			"set_meta_disk_size":             newSetMetaDiskSize(),
			"string_data_disk_size":          newStringDataDiskSize(),
			"string_meta_disk_size":          newStringMetaDiskSize(),
			"zset_data_disk_size":            newZsetDataDiskSize(),
			"zset_meta_disk_size":            newZsetMetaDiskSize(),
			"zset_index_disk_size":           newZsetIndexDiskSize(),
			"string_data_bithash_add_key":    newStringDataBithashAddKey(),
			"hash_data_bithash_add_key":      newHashDataBithashAddKey(),
			"list_data_bithash_add_key":      newListDataBithashAddKey(),
			"string_data_bithash_delete_key": newStringDataBithashDeleteKey(),
			"hash_data_bithash_delete_key":   newHashDataBithashDeleteKey(),
			"list_data_bithash_delete_key":   newListDataBithashDeleteKey(),
			"hash_data_free_page":            newHashDataFreePage(),
			"hash_meta_free_page":            newHashMetaFreePage(),
			"list_data_free_page":            newListDataFreePage(),
			"list_meta_free_page":            newListMetaFreePage(),
			"set_data_free_page":             newSetDataFreePage(),
			"set_meta_free_page":             newSetMetaFreePage(),
			"string_data_free_page":          newStringDataFreePage(),
			"string_meta_free_page":          newStringMetaFreePage(),
			"zset_data_free_page":            newZsetDataFreePage(),
			"zset_meta_free_page":            newZsetMetaFreePage(),
			"zset_index_free_page":           newZsetIndexFreePage(),
			"string_data_flush_mem_time":     newStringDataFlushMemTime(),
			"hash_data_flush_mem_time":       newHashDataFlushMemTime(),
			"set_data_flush_mem_time":        newSetDataFlushMemTime(),
			"list_data_flush_mem_time":       newListDataFlushMemTime(),
			"zset_data_flush_mem_time":       newZsetDataFlushMemTime(),
			"zset_index_flush_mem_time":      newZsetIndexFlushMemTime(),
			"runtime_heap_objects":           newRuntimeHeapObjects(),
			"runtime_heap_inuse":             newRuntimeHeapInuse(),
			"runtime_heap_idle":              newRuntimeHeapIdle(),
			"runtime_heap_alloc":             newRuntimeHeapAlloc(),
			"runtime_heap_sys":               newRuntimeHeapSys(),
			"runtime_general_alloc":          newRuntimeGeneralAlloc(),
			"runtime_general_sys":            newRuntimeGeneralSys(),
			"runtime_gc_num":                 newRuntimeGcNum(),
			"runtime_gc_total_pausems":       newRuntimeGcTotalPausems(),
			"runtime_num_goroutines":         newRuntimeNumGoroutines(),
			"server_cpu_throttled_nr":        newCpuThrottledNr(),
			"role":                           newRole(),
		},
	}
}

func (p *Bitalos) Describe(desc chan<- *prometheus.Desc) {
	for _, v := range p.gaugeVec {
		v.Describe(desc)
	}
}

func (p *Bitalos) Collect(metrics chan<- prometheus.Metric) {
	p.process2()
	for _, v := range p.gaugeVec {
		v.Collect(metrics)
	}
}

// @deprecated: collect and setMetric
func (p *Bitalos) process() {
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
					g.Process(i.IP, i.Name, i.Port, i.Idc, i.Group, v)
				}
			}
		}(v)
	}
	p.wg.Wait()
}

func (p *Bitalos) process2() {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, v := range p.instances {
		metrics := p.getCache(v)
		for k, val := range metrics {
			if g, ok := p.gaugeVec[k]; ok {
				g.Process(v.IP, v.Name, v.Port, v.Idc, v.Group, val)
			}
		}
	}
}

func (p *Bitalos) UpdateCpuLimit() {
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
		p.updateCacheOneField(n.IP, n.Port, "server_cpu_throttled_nr", n.CpuThrottledNr)
	}
}

func (p *Bitalos) CollectInfo() {
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
		}(v)
	}
	p.wg.Wait()
}

func (p *Bitalos) info(instance collector.Instance) (map[string]float64, error) {
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
	var isMaster bool
	if strings.Contains(do, "major_version") {
		c, _ := redis.String(conn.Do("clusterinfo"))
		isMaster = strings.Contains(c, "master")
	}
	return collector.ParseServerInfo(do, isMaster), nil
}

func (p *Bitalos) updateCache(ip, port string, metrics map[string]float64) {
	p.wmu.Lock()
	defer p.wmu.Unlock()

	if p.cache == nil {
		p.cache = make(map[string]map[string]internal.MetricList)
	}
	if p.cache[ip] == nil {
		p.cache[ip] = make(map[string]internal.MetricList)
	}
	p.cache[ip][port] = metrics
}

func (p *Bitalos) updateCacheOneField(ip, port string, metricName string, value float64) {
	if _, ok := p.cache[ip]; !ok {
		return
	}
	if _, ok := p.cache[ip][port]; !ok {
		return
	}
	p.cache[ip][port][metricName] = value
}

func (p *Bitalos) getCache(instance collector.Instance) map[string]float64 {
	if ipCache, ok := p.cache[instance.IP]; ok {
		if portCache, ok2 := ipCache[instance.Port]; ok2 {
			return portCache
		}
	}
	return nil
}
