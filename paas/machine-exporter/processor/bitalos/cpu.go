package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type cpu struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *cpu) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *cpu) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *cpu) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	cpuName = "cpu"
)

func newCpu() *cpu {
	return &cpu{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        cpuName,
		Help:        "CPU usage",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
