package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type hashDataFlushMemTime struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *hashDataFlushMemTime) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *hashDataFlushMemTime) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *hashDataFlushMemTime) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	hashDataFlushMemTimeName = "hash_data_flush_mem_time"
)

func newHashDataFlushMemTime() *hashDataFlushMemTime {
	return &hashDataFlushMemTime{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        hashDataFlushMemTimeName,
		Help:        "hashDataFlushMemTime",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
