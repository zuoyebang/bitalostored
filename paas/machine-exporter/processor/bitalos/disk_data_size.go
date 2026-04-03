package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type diskDataSize struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *diskDataSize) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *diskDataSize) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *diskDataSize) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	diskDataSizeName = "disk_data_size"
)

func newDiskDataSize() *diskDataSize {
	return &diskDataSize{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        diskDataSizeName,
		Help:        "disk_data_size",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
