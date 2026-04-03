package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type instantaneousOpsPerSec struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *instantaneousOpsPerSec) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *instantaneousOpsPerSec) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *instantaneousOpsPerSec) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	instantaneousOpsPerSecName = "instantaneous_ops_per_sec"
)

func newInstantaneousOpsPerSec() *instantaneousOpsPerSec {
	return &instantaneousOpsPerSec{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        instantaneousOpsPerSecName,
		Help:        "Command QPS",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
