package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type setDataFlushMemTime struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *setDataFlushMemTime) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *setDataFlushMemTime) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *setDataFlushMemTime) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	setDataFlushMemTimeName = "set_data_flush_mem_time"
)

func newSetDataFlushMemTime() *setDataFlushMemTime {
	return &setDataFlushMemTime{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        setDataFlushMemTimeName,
		Help:        "setDataFlushMemTime",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
