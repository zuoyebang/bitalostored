package bitalos

import (
	"machine-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
)

type zsetMetaFreePage struct {
	gaugeVec *prometheus.GaugeVec
}

func (c *zsetMetaFreePage) Describe(desc chan<- *prometheus.Desc) {
	c.gaugeVec.Describe(desc)
}

func (c *zsetMetaFreePage) Collect(metrics chan<- prometheus.Metric) {
	c.gaugeVec.Collect(metrics)
}

func (c *zsetMetaFreePage) Process(machine string, name string, port string, idc string, group string, v float64) {
	c.gaugeVec.WithLabelValues(machine, name, port, idc, group).Set(v)
}

var (
	zsetMetaFreePageName = "zset_meta_free_page"
)

func newZsetMetaFreePage() *zsetMetaFreePage {
	return &zsetMetaFreePage{gaugeVec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   collector.Namespace,
		Subsystem:   collector.SubSystem,
		Name:        zsetMetaFreePageName,
		Help:        "zset_meta_free_page",
		ConstLabels: map[string]string{"type": "bitalos"},
	}, collector.ServerLabels)}
}
