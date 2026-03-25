// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"bytes"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/log"
	"io"
	"net/http"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

const (
	//Proxy
	MetricNameProxyQps = "stored_machine_cdm_ops_qps"
	MetricNameProxyCpu = "stored_machine_rusage_cpu"
	MetricNameProxyMem = "stored_machine_rusage_mem"

	//Server
	MetricNameServerQps = "stored_machine_instantaneous_ops_per_sec"
	MetricNameServerCpu = "stored_machine_cpu"
	MetricNameServerMem = "stored_machine_memory_total"

	//DISK
	MetricNameDisk = "stored_machine_disk_used_size"

	MetricNameServerRole = "stored_machine_role"
)

type MachineInfo struct {
	ServerCluster map[string]*ServerMetric `json:"serverCluster"`
	ProxyCluster  map[string]*ProxyMetric  `json:"proxyCluster"`
}

type ServerMetric struct {
	MasterNum uint8 `json:"masterNum"`
}

type ProxyMetric struct {
}

type GrafanaCollector struct {
	GrafanaUrl string
}

var allMachineMetrics map[string]*MachineInfo

func GetAllMachineMetrics() map[string]*MachineInfo {
	return allMachineMetrics
}

func GetServerMasterNum(ip, cluster string) uint8 {
	if v, ok := allMachineMetrics[ip]; ok {
		if v2, ok2 := v.ServerCluster[cluster]; ok2 {
			return v2.MasterNum
		}
	}
	return 0
}

func NewGrafanaCollector() *GrafanaCollector {
	g := &GrafanaCollector{
		GrafanaUrl: config.GetConf().PaasServer.GrafanaUrl,
	}
	g.run()
	return g
}

func (g *GrafanaCollector) run() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Infof("query grafana error. panic:%+v stack:%s", r, debug.Stack())
			}
			g.run()
		}()
		for {
			allMachineMetrics, _ = g.queryGrafana()
			time.Sleep(5 * time.Minute)
		}
	}()
}

func (g *GrafanaCollector) queryGrafana() (map[string]*MachineInfo, error) {
	resp, err := http.Get(g.GrafanaUrl)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(data)
	machineMetrics := make(map[string]*MachineInfo, 0)
	for {
		line, err := buf.ReadBytes('\n')
		if err != nil {
			break
		}

		l := string(line)
		i := strings.Index(l, "{")
		if i == -1 {
			continue
		}
		metricName := l[0:i]
		_, ip, clusterName, original := formatLineInfo(l)
		if _, ok := machineMetrics[ip]; !ok {
			m := &MachineInfo{}
			m.ServerCluster = make(map[string]*ServerMetric, 0)
			m.ServerCluster[clusterName] = &ServerMetric{}
			machineMetrics[ip] = m
		}
		if _, ok := machineMetrics[ip].ServerCluster[clusterName]; !ok {
			machineMetrics[ip].ServerCluster[clusterName] = &ServerMetric{}
		}
		switch metricName {
		case MetricNameServerRole:
			if int(original) == 1 {
				machineMetrics[ip].ServerCluster[clusterName].MasterNum++
			}
		}
	}
	return machineMetrics, nil
}

func formatLineInfo(l string) (idc, machine, clusterName string, original float64) {
	re := regexp.MustCompile(`idc="([^"]+)",machine="([\d|\.]*)",.*name="([^"]+)",.*\} ([\d.]+(?:[eE][+\-]?\d+)?)`)
	match := re.FindStringSubmatch(l)
	if len(match) == 5 {
		idc = match[1]
		machine = match[2]
		clusterName = match[3]

		value := match[4]
		original, _ = strconv.ParseFloat(value, 64)
	}
	return
}
