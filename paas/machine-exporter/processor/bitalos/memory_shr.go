package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type memoryShared struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *memoryShared) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *memoryShared) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *memoryShared) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	memorySharedName = "memory_shr"
)

func newMemoryShared() *memoryShared {
	return &memoryShared{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        memorySharedName,
		Help:        "Shared memory",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
