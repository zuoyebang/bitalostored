package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type zsetIndexFlushMemTime struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *zsetIndexFlushMemTime) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *zsetIndexFlushMemTime) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *zsetIndexFlushMemTime) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	zsetIndexFlushMemTimeName = "zset_index_flush_mem_time"
)

func newZsetIndexFlushMemTime() *zsetIndexFlushMemTime {
	return &zsetIndexFlushMemTime{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        zsetIndexFlushMemTimeName,
		Help:        "zsetIndexFlushMemTime",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
