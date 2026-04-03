package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type stringDataBithashDeleteKey struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *stringDataBithashDeleteKey) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *stringDataBithashDeleteKey) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *stringDataBithashDeleteKey) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	stringDataBithashDeleteKeyName = "string_data_bithash_delete_key"
)

func newStringDataBithashDeleteKey() *stringDataBithashDeleteKey {
	return &stringDataBithashDeleteKey{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        stringDataBithashDeleteKeyName,
		Help:        "string_data_bithash_delete_key",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
