package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type listDataFreePage struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *listDataFreePage) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *listDataFreePage) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *listDataFreePage) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	listDataFreePageName = "list_data_free_page"
)

func newListDataFreePage() *listDataFreePage {
	return &listDataFreePage{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        listDataFreePageName,
		Help:        "list_data_free_page",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
