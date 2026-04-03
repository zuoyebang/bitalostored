package proxy

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type rUsageMem struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *rUsageMem) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *rUsageMem) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *rUsageMem) Process(machine string, name string, port string, idc string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc).Set(v)
}

var (
	rUsageMemName = "rusage_mem"
)

func newRUsageMem() *rUsageMem {
	return &rUsageMem{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        rUsageMemName,
		Help:        "Memory usage rate",
		ConstLabels: map[string]string{"type": "proxy"},
	}, collector.ProxyLabels)}
}
