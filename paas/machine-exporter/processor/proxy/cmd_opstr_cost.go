package proxy

import (
	"github.com/prometheus/client_golang/prometheus"
	"machine-exporter/collector"
	"strings"
)

type cmdOpStrCost struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *cmdOpStrCost) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *cmdOpStrCost) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *cmdOpStrCost) Process(machine string, name string, port string, idc string, v float64) {
	mixed := strings.Split(idc, "_")
	idc = mixed[0]
	cmd := mixed[1]
	c.gaugeVec.WithLabelValues(machine, name, port, idc, cmd).Set(v)
}

var (
	CmdOpStrCostName = "cmd_opstr_cost"
)

func newCmdOpStrCost() *cmdOpStrCost {
	return &cmdOpStrCost{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        CmdOpStrCostName,
		Help:        "Latency for each command",
		ConstLabels: map[string]string{"type": "proxy"},
	}, collector.ProxyLabelsWithCmd)}
}
