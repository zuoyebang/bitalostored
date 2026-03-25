package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type runtimeHeapInuse struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *runtimeHeapInuse) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *runtimeHeapInuse) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *runtimeHeapInuse) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	runtimeHeapInuseName = "runtime_heap_inuse"
)

func newRuntimeHeapInuse() *runtimeHeapInuse {
	return &runtimeHeapInuse{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        runtimeHeapInuseName,
		Help:        "",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
