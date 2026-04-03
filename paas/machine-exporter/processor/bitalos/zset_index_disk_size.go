package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type zsetIndexDiskSize struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *zsetIndexDiskSize) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *zsetIndexDiskSize) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *zsetIndexDiskSize) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	zsetIndexDiskSizeName = "zset_index_disk_size"
)

func newZsetIndexDiskSize() *zsetIndexDiskSize {
	return &zsetIndexDiskSize{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        zsetIndexDiskSizeName,
		Help:        "zset_index_disk_size",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
