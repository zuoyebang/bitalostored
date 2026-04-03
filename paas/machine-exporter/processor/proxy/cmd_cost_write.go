package proxy

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type cmdCostWrite struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *cmdCostWrite) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *cmdCostWrite) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *cmdCostWrite) Process(machine string, name string, port string, idc string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc).Set(v)
}

var (
	cmdCostWriteName = "cmd_cost_write"
)

func newCmdCostWrite() *cmdCostWrite {
	return &cmdCostWrite{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        cmdCostWriteName,
		Help:        "Write latency",
		ConstLabels: map[string]string{"type": "proxy"},
	}, collector.ProxyLabels)}
}
