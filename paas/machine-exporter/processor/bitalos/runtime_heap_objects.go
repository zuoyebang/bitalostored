package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type runtimeHeapObjects struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *runtimeHeapObjects) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *runtimeHeapObjects) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *runtimeHeapObjects) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	runtimeHeapObjectsName = "runtime_heap_objects"
)

func newRuntimeHeapObjects() *runtimeHeapObjects {
	return &runtimeHeapObjects{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        runtimeHeapObjectsName,
		Help:        "",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
