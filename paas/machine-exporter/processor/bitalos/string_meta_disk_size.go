package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type stringMetaDiskSize struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *stringMetaDiskSize) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *stringMetaDiskSize) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *stringMetaDiskSize) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	stringMetaDiskSizeName = "string_meta_disk_size"
)

func newStringMetaDiskSize() *stringMetaDiskSize {
	return &stringMetaDiskSize{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        stringMetaDiskSizeName,
		Help:        "string_meta_disk_size",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
