package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type runtimeGcTotalPausems struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *runtimeGcTotalPausems) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *runtimeGcTotalPausems) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *runtimeGcTotalPausems) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	runtimeGcTotalPausemsName = "runtime_gc_total_pausems"
)

func newRuntimeGcTotalPausems() *runtimeGcTotalPausems {
	return &runtimeGcTotalPausems{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        runtimeGcTotalPausemsName,
		Help:        "",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
