package proxy

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type cmdCostAvg struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *cmdCostAvg) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *cmdCostAvg) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *cmdCostAvg) Process(machine string, name string, port string, idc string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc).Set(v)
}

var (
	cmdCostAvgName = "cmd_cost_avg"
)

func newCmdCostAvg() *cmdCostAvg {
	return &cmdCostAvg{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        cmdCostAvgName,
		Help:        "Average command latency",
		ConstLabels: map[string]string{"type": "proxy"},
	}, collector.ProxyLabels)}
}
