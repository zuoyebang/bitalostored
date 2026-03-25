package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type hashDataBithashDeleteKey struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *hashDataBithashDeleteKey) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *hashDataBithashDeleteKey) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *hashDataBithashDeleteKey) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	hashDataBithashDeleteKeyName = "hash_data_bithash_delete_key"
)

func newHashDataBithashDeleteKey() *hashDataBithashDeleteKey {
	return &hashDataBithashDeleteKey{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        hashDataBithashDeleteKeyName,
		Help:        "hash_data_bithash_delete_key",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
