package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type listDataBithashDeleteKey struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *listDataBithashDeleteKey) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *listDataBithashDeleteKey) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *listDataBithashDeleteKey) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	listDataBithashDeleteKeyName = "list_data_bithash_delete_key"
)

func newListDataBithashDeleteKey() *listDataBithashDeleteKey {
	return &listDataBithashDeleteKey{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        listDataBithashDeleteKeyName,
		Help:        "list_data_bithash_delete_key",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
