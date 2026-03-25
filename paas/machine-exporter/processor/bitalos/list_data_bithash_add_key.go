package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type listDataBithashAddKey struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *listDataBithashAddKey) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *listDataBithashAddKey) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *listDataBithashAddKey) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	listDataBithashAddKeyName = "list_data_bithash_add_key"
)

func newListDataBithashAddKey() *listDataBithashAddKey {
	return &listDataBithashAddKey{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        listDataBithashAddKeyName,
		Help:        "list_data_bithash_add_key",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
