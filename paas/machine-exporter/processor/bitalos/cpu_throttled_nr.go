package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type cpuThrottledNr struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *cpuThrottledNr) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *cpuThrottledNr) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *cpuThrottledNr) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	cpuThrottledNrName = "server_cpu_throttled_nr"
)

func newCpuThrottledNr() *cpuThrottledNr {
	return &cpuThrottledNr{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        cpuThrottledNrName,
		Help:        "server_cpu_throttled_nr",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
