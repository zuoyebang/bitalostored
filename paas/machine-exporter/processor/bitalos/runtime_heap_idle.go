package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type runtimeHeapIdle struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *runtimeHeapIdle) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *runtimeHeapIdle) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *runtimeHeapIdle) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	runtimeHeapIdleName = "runtime_heap_idle"
)

func newRuntimeHeapIdle() *runtimeHeapIdle {
	return &runtimeHeapIdle{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        runtimeHeapIdleName,
		Help:        "",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
