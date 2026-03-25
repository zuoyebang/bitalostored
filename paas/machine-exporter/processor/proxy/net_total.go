package proxy

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type netTotal struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *netTotal) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *netTotal) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *netTotal) Process(machine string, name string, port string, idc string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc).Set(v)
}

var (
	netTotalName = "net_total"
)

func newNetTotal() *netTotal {
	return &netTotal{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        netTotalName,
		Help:        "netTotal",
		ConstLabels: map[string]string{"type": "proxy"},
	}, collector.ProxyLabels)}
}
