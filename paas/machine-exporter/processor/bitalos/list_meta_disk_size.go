package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type listMetaDiskSize struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *listMetaDiskSize) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *listMetaDiskSize) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *listMetaDiskSize) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	listMetaDiskSizeName = "list_meta_disk_size"
)

func newListMetaDiskSize() *listMetaDiskSize {
	return &listMetaDiskSize{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        listMetaDiskSizeName,
		Help:        "list_meta_disk_size",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
