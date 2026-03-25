package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type diskUsedHumanSize struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *diskUsedHumanSize) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *diskUsedHumanSize) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *diskUsedHumanSize) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	diskUsedHumanSizeName = "disk_used_human_size"
)

func newDiskUsedHumanSize() *diskUsedHumanSize {
	return &diskUsedHumanSize{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        diskUsedHumanSizeName,
		Help:        "disk_used_human_size",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
