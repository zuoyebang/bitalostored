package proxy

import (
	"github.com/prometheus/client_golang/prometheus"
	"machine-exporter/collector"
	"strings"
)

type cmdOpStrQps struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *cmdOpStrQps) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *cmdOpStrQps) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *cmdOpStrQps) Process(machine string, name string, port string, idc string, v float64) {
	mixed := strings.Split(idc, "_")
	idc = mixed[0]
	cmd := mixed[1]
	c.gaugeVec.WithLabelValues(machine, name, port, idc, cmd).Set(v)
}

var (
	CmdOpStrQpsName = "cmd_opstr_qps"
)

func newCmdOpStrQps() *cmdOpStrQps {
	return &cmdOpStrQps{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        CmdOpStrQpsName,
		Help:        "QPS for each command",
		ConstLabels: map[string]string{"type": "proxy"},
	}, collector.ProxyLabelsWithCmd)}
}
