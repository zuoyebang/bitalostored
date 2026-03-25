package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type zsetDataFlushMemTime struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *zsetDataFlushMemTime) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *zsetDataFlushMemTime) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *zsetDataFlushMemTime) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	zsetDataFlushMemTimeName = "zset_data_flush_mem_time"
)

func newZsetDataFlushMemTime() *zsetDataFlushMemTime {
	return &zsetDataFlushMemTime{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        zsetDataFlushMemTimeName,
		Help:        "zsetDataFlushMemTime",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
