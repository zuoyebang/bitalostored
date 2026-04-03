package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type memoryTotal struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *memoryTotal) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *memoryTotal) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *memoryTotal) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	memoryTotalName = "memory_total"
)

func newMemoryTotal() *memoryTotal {
	return &memoryTotal{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        memoryTotalName,
		Help:        "Memory usage",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
