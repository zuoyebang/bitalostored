package proxy

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type cmdCostRead struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *cmdCostRead) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *cmdCostRead) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *cmdCostRead) Process(machine string, name string, port string, idc string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc).Set(v)
}

var (
	cmdCostReadName = "cmd_cost_read"
)

func newCmdCostRead() *cmdCostRead {
	return &cmdCostRead{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        cmdCostReadName,
		Help:        "Read latency",
		ConstLabels: map[string]string{"type": "proxy"},
	}, collector.ProxyLabels)}
}
