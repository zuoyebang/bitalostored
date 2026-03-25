package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type stringDataFlushMemTime struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *stringDataFlushMemTime) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *stringDataFlushMemTime) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *stringDataFlushMemTime) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	stringDataFlushMemTimeName = "string_data_flush_mem_time"
)

func newStringDataFlushMemTime() *stringDataFlushMemTime {
	return &stringDataFlushMemTime{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        stringDataFlushMemTimeName,
		Help:        "stringDataFlushMemTime",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
