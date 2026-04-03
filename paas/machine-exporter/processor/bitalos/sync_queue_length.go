package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type syncQueueLength struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *syncQueueLength) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *syncQueueLength) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *syncQueueLength) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	syncQueueLengthName = "sync_queue_length"
)

func newSyncQueueLength() *syncQueueLength {
	return &syncQueueLength{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        syncQueueLengthName,
		Help:        "Async queue length",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
