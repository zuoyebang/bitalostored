package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type listMetaFreePage struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *listMetaFreePage) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *listMetaFreePage) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *listMetaFreePage) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	listMetaFreePageName = "list_meta_free_page"
)

func newListMetaFreePage() *listMetaFreePage {
	return &listMetaFreePage{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        listMetaFreePageName,
		Help:        "list_meta_free_page",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
