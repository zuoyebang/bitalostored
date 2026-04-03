package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type setDataFreePage struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *setDataFreePage) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *setDataFreePage) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *setDataFreePage) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	setDataFreePageName = "set_data_free_page"
)

func newSetDataFreePage() *setDataFreePage {
	return &setDataFreePage{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        setDataFreePageName,
		Help:        "set_data_free_page",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
