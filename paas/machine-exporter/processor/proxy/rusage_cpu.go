package proxy

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type rUsageCpu struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *rUsageCpu) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *rUsageCpu) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *rUsageCpu) Process(machine string, name string, port string, idc string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc).Set(v)
}

var (
	rUsageCpuName = "rusage_cpu"
)

func newRUsageCpu() *rUsageCpu {
	return &rUsageCpu{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        rUsageCpuName,
		Help:        "CPU usage rate",
		ConstLabels: map[string]string{"type": "proxy"},
	}, collector.ProxyLabels)}
}
