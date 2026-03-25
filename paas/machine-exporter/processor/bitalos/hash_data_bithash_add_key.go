package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type hashDataBithashAddKey struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *hashDataBithashAddKey) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *hashDataBithashAddKey) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *hashDataBithashAddKey) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	hashDataBithashAddKeyName = "hash_data_bithash_add_key"
)

func newHashDataBithashAddKey() *hashDataBithashAddKey {
	return &hashDataBithashAddKey{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        hashDataBithashAddKeyName,
		Help:        "hash_data_bithash_add_key",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
