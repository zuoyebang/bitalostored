package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type stringDataFreePage struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *stringDataFreePage) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *stringDataFreePage) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *stringDataFreePage) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	stringDataFreePageName = "string_data_free_page"
)

func newStringDataFreePage() *stringDataFreePage {
	return &stringDataFreePage{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        stringDataFreePageName,
		Help:        "string_data_free_page",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
