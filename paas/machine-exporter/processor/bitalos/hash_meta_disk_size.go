package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type hashMetaDiskSize struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *hashMetaDiskSize) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *hashMetaDiskSize) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *hashMetaDiskSize) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	hashMetaDiskSizeName = "hash_meta_disk_size"
)

func newHashMetaDiskSize() *hashMetaDiskSize {
	return &hashMetaDiskSize{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        hashMetaDiskSizeName,
		Help:        "hash_meta_disk_size",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
